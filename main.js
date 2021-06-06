'use strict';
const config  = require('./config');
const Bottleneck = require('bottleneck');
const https = require('https');
const mysql = require('mysql');
const schedule = require('node-schedule');
const countdown = require('countdown');
const log = require('single-line-log').stdout;
const timediff = require('timediff');
var day;

var max;
var count;
var list = [], queries = {}, sql;
var keyType = "Main";
var scanStart, scanEnd;
const limiter = new Bottleneck({
  maxConcurrent: 1,
  minTime: 333
});

limiter.on("executing", function(){
  console.log(limiter.jobs("EXECUTING").join(", ")+" Executing");
})

limiter.on("failed", async (error, jobInfo) => {
  const id = jobInfo.options.id;
  console.warn(`Job ${id} failed: ${error}`);

  if (jobInfo.retryCount === 0) {
    console.log(`Retrying job ${id} in 25ms!`);
    return 25;
  }
});

limiter.on("retry", (error, jobInfo) => console.log(`Now retrying ${jobInfo.options.id}`));

var con = mysql.createPool({
  host: config.MysqlHost,
  user: config.MysqlUsername,
  password: config.MysqlPassword,
  database: config.MysqlDatabase
});

con.getConnection(function(err, connection) {
  if (err) throw err;
  console.log("Connected to database");
  timeToJob.start();
});

function getKey(){
  return new Promise(callback =>{
    var apiKey;
    const sql = "SELECT id, apiKey, count FROM apiKeys WHERE note like '%"+keyType+"%' GROUP BY id, apiKey, count ORDER BY count desc LIMIT 1";
    con.query(sql, function (err, result, fields){
      if(err) throw err;
      apiKey = result[0].apiKey;
      var id = result[0].id;
      const sql = "UPDATE apiKeys SET count = count-1 WHERE id = "+id;
      con.query(sql, function (err, result, fields){
        if(err) throw err;
        callback(apiKey);
      })
    });
  })
}

schedule.scheduleJob('1 22 * * *', function(){
  timeToJob.stop();
  saveParam((Date.now()+86400000), 2);
  console.log("--- RUNNING JOB ---");
  init();
});

function saveParam(val, id){
  sql = "UPDATE persist SET param = '"+val+"' WHERE id = "+id+";";
  con.query(sql, function(err, result, fields){
    if(err) throw err;
  })
}

function init(){
  scanStart = Date.now();
  persist(1).then((param) => {
    updateQueries().then(() => {
      console.log(queries.available+" Searches available today.");
      count = 0;
      users(parseInt(param));
    });
  })
}

function persist(id){
  return new Promise(callback => {
    sql = "SELECT param FROM persist WHERE id = "+id;
    con.query(sql, function(err, result, fields){
      if(err) throw err;

      callback(result[0].param);
    })
  })
}

function updateQueries(){
  return new Promise(callback => {
    sql = "SELECT id, count FROM apiKeys WHERE note like '%"+keyType+"%'";
    con.query(sql, function(err, result, fields){
      if(err) throw err;
      queries.data = result;
      queries.available = 0;
      for(var x = 0; x < queries.data.length; x++){
        queries.available += queries.data[x].count;
      }
      callback();
    })
  })
}

function users(param){
  sql = "SELECT username FROM `CACHE players` WHERE event = 'First Entry';";
  con.query(sql, function(err, result, fields){
    if(err) throw err;
    list = result;
    update(param);
  })
}

async function update(param = 0){
  max = today();
  var end = param + today();
  console.log(today());
  console.log("Updating "+today()+" users today \n#"+param+" to #"+end);
  //end
  for(var i = param; i < end; i++){
    await getKey().then((key) => {
      limiter.schedule( {id:list[i].username}, async ()=>{
        await queryApi(list[i].username, key)
        .then((result)=>{
          if(result.status == 0){
            throw new Error(result.data);
          }
          if(count == max){
            finish();
          }
        });
      }).catch((error) => {
        if (error instanceof Bottleneck.BottleneckError) {

        }
      });
    });
    saved = i;
  }
  saveParam(end, 1);
}

const queryApi = function(username, key){
  return new Promise(callback => {
    var options = {
      hostname: 'api.starcitizen-api.com',
      port: 443,
      path: '/'+key+'/v1/live/user/'+escape(username),
      method: 'GET'
    }
    const req = https.request(options, res =>{
      var body = "";
      res.on('data', d => {
        body += d;
      })
      res.on('error', error => {
        console.error(error);
      })
      res.on('end', function(){
        count++;
        try{
          var user = JSON.parse(body);
          if(user.data == null){
            callback({status:0, data:args+" returned null, retrying"});
          }
        }catch(err){
          var result = "Encountered an error, User: "+username;
          promiseSearch({ status:0, data:result });
        };
        if(Object.size(user.data) > 0){
          cachePlayer(user.data);
          callback({ status:1 });
        }else{
          console.log("Error: "+username);
          callback({ status:0, data:"Error: "+username });
        }
      })
    });
    req.end()
  });
}

var saved = 0;

function today(){
  var weeks = 1;
  var temp = Math.round(list.length/(7*weeks));
  while(temp > (queries.available/2)){
    temp = Math.round(list.length/(7*++weeks));
  }
  return parseInt(temp);
}

function cachePlayer(user){
  //console.log(con.escape(user.profile.bio));
  var update = false;
  var eventUpdate = new Array();
  var check = { cID:0,
                username:'',
                badge: { src:'', title:'' },
                organization: [],
                avatar: ''
              };
  check.cID = parseInt(user.profile.id.substring(1));
  check.username = user.profile.handle;
  check.badge.title = user.profile.badge;
  check.badge.src = user.profile.badge_image;
  check.avatar = user.profile.image;
  if(Object.size(user.affiliation) > 0){
    user.orgLength = Object.size(user.affiliation) + 1;
  }
  if(user.organization.sid){
    check.organization.push({ sid: user.organization.sid, rank: user.organization.stars });
  }else{
    check.organization.push({ sid: "N/A", rank: 0 });
  }
  for(var i = 0; i < Object.size(user.affiliation); i++){
    if(user.affiliation[i].sid){
      check.organization.push({ sid: user.affiliation[i].sid, rank: user.affiliation[i].stars });
    }else{
      check.organization.push({ sid: "N/A", rank: 0 });
    }
  }
  var sql = "";
  if(check.cID){
    sql = "SELECT cID, username, badge, organization, avatar FROM `CACHE players` WHERE cID = "+user.profile.id.substring(1)+";";
  }else{
    check.cID = 0;
    sql = "SELECT cID, username, badge, organization, avatar FROM `CACHE players` WHERE username = '"+user.profile.handle+"';";
  }
  con.query(sql, function (err, result, fields) {
    if(err) throw err;

    if(Object.size(result) > 0){
      var data = result[result.length-1];
      data.organization = JSON.parse(data.organization);
      data.organization = Object.values(data.organization);
      data.badge = JSON.parse(data.badge);
      for(var i = 0; i < Object.size(data); i++){
        if(i == 3){
          for(var x = 0; x < Object.size(data.organization) && x < Object.size(check.organization); x++){
            if(data.organization[x].sid != check.organization[x].sid){
              update = true;
              eventUpdate.push("Org Change");
            }else if(data.organization[x].rank != check.organization[x].rank){
              update = true;
              eventUpdate.push("Org Promotion/Demotion");
            }
          }
        }
      }
      if(data.cID != check.cID){
        update = true;
        eventUpdate.push("Obtained ID");
      }
      if(data.username != check.username){
        update = true;
        eventUpdate.push("Username Changed");
      }else if (data.badge.title != check.badge.title) {
        update = true;
        eventUpdate.push("Badge Changed");
      }else if (data.avatar != check.avatar) {
        update = true;
        eventUpdate.push("Avatar Changed");
      }
      eventUpdate = eventUpdate.filter((c, index) => {
          return eventUpdate.indexOf(c) === index;
      });
      console.log(eventUpdate);
    }else{
      check.badge = JSON.stringify(check.badge);
      check.organization = JSON.stringify(Object.assign({}, check.organization));
      const sql = "INSERT INTO `CACHE players` (event, cID, username, badge, organization, avatar) VALUES ('First Entry', "+check.cID+", '"+check.username+"', '"+check.badge+"', '"+check.organization+"', '"+check.avatar+"' );";
      con.query(sql, function (err, result, fields) {
        if(err){
          console.log(err);
        }
      });
    }
    if(update){
      check.badge = JSON.stringify(check.badge);
      check.organization = JSON.stringify(Object.assign({}, check.organization));
      var eventString = eventUpdate.join(", ");
      const sql = "INSERT INTO `CACHE players` (event, cID, username, badge, organization, avatar) VALUES ('"+eventString+"', "+check.cID+", '"+check.username+"', '"+check.badge+"', '"+check.organization+"', '"+check.avatar+"');";
      con.query(sql, function (err, result, fields) {
        if(err) throw err;
      });
    }
  });
}

function timeToComplete(){
  return new Promise(callback =>{
    sql = "SELECT username FROM `CACHE players` WHERE event = 'First Entry';";
    con.query(sql, function(err, result, fields){
      if(err) throw err;
      var max = result.length, remaining = 0, time = 0, available = queries.data.length*500;
      persist(1).then((param) =>{
        remaining = max - param;
        time = Math.ceil(remaining/available);
        console.log("Approximately "+time+" days to completion");
        callback();
      });
    })
  })
}

function Timer(fn, t) {
    var timerObj = setInterval(fn, t);

    this.stop = function() {
        if (timerObj) {
            clearInterval(timerObj);
            timerObj = null;
            log.clear();
            console.log("");
        }
        return this;
    }
    this.start = function() {
        persist(2).then((param) => {
          day = parseInt(param);
          if (!timerObj) {
              this.stop();
              timerObj = setInterval(fn, t);
          }
          return this;
        });
    }
    this.reset = function(newT = t) {
        t = newT;
        return this.stop().start();
    }
}

function calcTime(){
  const timeLeft = countdown(Date.now(), day);
  log("Running job in "+timeLeft.hours+":"+timeLeft.minutes+":"+timeLeft.seconds);
}

const timeToJob = new Timer(calcTime, 500);

Object.size = function(obj) {
  var size = 0, key;
  for (key in obj) {
      if (obj.hasOwnProperty(key)) size++;
  }
  return size;
};

function finish(){
  scanEnd = Date.now();
  var runtime = timediff(scanStart, scanEnd);
  console.log("Finished scanning "+max+" players \nRuntime: "+runtime.hours+":"+runtime.minutes+":"+runtime.seconds+"."+runtime.milliseconds);
  timeToComplete().then(()=>{
    timeToJob.start();
  });
}
