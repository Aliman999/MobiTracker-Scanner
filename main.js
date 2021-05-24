'use strict';
const config  = require('./config');
const https = require('https');
const mysql = require('mysql');
const Bottleneck = require('bottleneck');
const schedule = require('node-schedule');
var timediff = require('timediff');

var con = mysql.createPool({
  host: config.MysqlHost,
  user: config.MysqlUsername,
  password: config.MysqlPassword,
  database: config.MysqlDatabase
});

con.getConnection(function(err, connection) {
  if (err) throw err;
});
var max;
var count;
var list = [], queries = {}, sql;
var keyType = "main";
var scanStart, scanEnd;

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

function persist(){
  return new Promise(callback => {
    sql = "SELECT param FROM persist WHERE id = 1";
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
      queries.available = function(){
        var temp = 0;
        for(var i = 0; i < this.data.length; i++){
          temp =+ this.data[i].count;
        }
        return temp*this.data.length;
      }
      callback();
    })
  })
}

function init(){
  scanStart = Date.now();
  persist().then((param) => {
    updateQueries().then(() => {
      console.log(queries.available()+" Searches available today.");
      count = 0;
      users(parseInt(param));
    });
  })
}


const limiter = new Bottleneck({
  maxConcurrent: 1,
  minTime: 333
});

function users(param){
  sql = "SELECT username FROM `CACHE players` WHERE event = 'First Entry';";
  console.log(sql);
  con.query(sql, function(err, result, fields){
    if(err) throw err;
    list = result;
    update(param);
  })
}

Object.size = function(obj) {
  var size = 0, key;
  for (key in obj) {
      if (obj.hasOwnProperty(key)) size++;
  }
  return size;
};

const queryApi = function(username, key){
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
      queryApi(username, key);
    })
    res.on('end', function(){
      count++;
      try{
        var user = JSON.parse(body);
        console.log("Found "+username+" using Key: "+key);
      }catch(err){
        console.log("Failed to parse "+username);
        console.log("");
        return;
      };
      if(Object.size(user.data) > 0){
        cachePlayer(user.data);
      }
      if(count == max){
        console.log(count+" of "+max+" scanned");
        scanEnd = Date.now();
        var runtime = timediff(scanStart, scanEnd);
        console.log("Finished scanning "+max+" players \nRuntime: "+runtime.hours+":"+runtime.minutes+":"+runtime.seconds+"."+runtime.milliseconds);
      }else{
        console.log(count+" of "+max+" scanned");
      }
    })
  });
  req.end()
}

var saved = 0;

function today(){
  var weeks = 1;
  var temp = Math.round(list.length/(7*weeks));
  while(temp > (queries.available()/2)){
    temp = Math.round(list.length/(7*++weeks));
  }
  return parseInt(temp);
}

async function update(param = 0){
  max = today();
  var end = param + today();
  console.log(today());
  console.log("Updating "+today()+" users today \n#"+param+" to #"+end);
  for(var i = param; i < end; i++){
    await getKey().then((key) => {
      limiter.schedule(queryApi, list[i].username, key);
    });
    saved = i;
  }
  saveParam(end);
}

function saveParam(val){
  sql = "UPDATE persist SET param = '"+val+"' WHERE id = 1;";
  con.query(sql, function(err, result, fields){
    if(err) throw err;
  })
}


schedule.scheduleJob('1 22 * * *', function(){
  init();
});

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
