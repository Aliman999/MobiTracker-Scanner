'use strict';
const config  = require('./config');
const Bottleneck = require('bottleneck');
const https = require('https');
const mysql = require('mysql');
const schedule = require('node-schedule');
const countdown = require('countdown');
const log = require('single-line-log').stdout;

var day;
var max;
var count;
var list = [], queries = {}, sql;
var keyType = "Main";
var offset = 1;
const limiter = new Bottleneck({
  maxConcurrent: offset,
  minTime: (offset*1000)
});


const orgScan = new Bottleneck({
  maxConcurrent: 1,
  minTime: 2000
});

const orgLimiter = new Bottleneck({
  maxConcurrent: 1,
  minTime: 2000
});

orgLimiter.on("failed", async (error, info) => {
  const id = info.options.id;
  console.warn(`${id} failed: ${error}`);

  if (info.retryCount < 3) {
    return 2000;
  }
});

orgLimiter.on("done", function(info){
  console.log("Returned data for "+info.options.id);
});

orgScan.on("failed", async (error, info) => {
  const id = info.options.id;
  console.warn(`${id} failed: ${error}`);

  if (info.retryCount < 2) {
    return 2000;
  }else{
    info.args[2].send(JSON.stringify({
      type:"response",
      data:info.args[0]+" not found.",
      message:"Error",
      status:0
    }));
    cachePlayer(info.args[0]);
  }
});

orgScan.on("done", function(info){
  console.log("Returned data for "+info.options.id);
});

limiter.on("failed", async (error, info) => {
  const id = info.options.id;
  console.warn(`${id} failed: ${error}`);

  if (info.retryCount < 2) {
    return (offset*1000);
  }else{
    console.log(info.args[2]+" of "+list.length+" | "+info.args[0]);
    cachePlayer(info.args[0]);
  }
});

limiter.on("done", function(info){
  count++;
  max--;
  console.log(info.args[2]+" of "+list.length+" | "+info.args[0]);
  if(count == max){
    if((info.args[2]+1) == list.length){
      finish(true);
    }else{
      finish();
    }
  }
});

//limiter.on("retry", (error, info) => console.log(`Retrying ${info.options.id}`));

var con = mysql.createPool({
  host: config.MysqlHost,
  user: config.MysqlUsername,
  password: config.MysqlPassword,
  database: config.MysqlDatabase
});

con.getConnection(function(err, connection){
  if (err) throw err;
  console.log("Connected to database");
  init();
});

function getKey(i){
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
        callback(apiKey, i);
      })
    });
  })
}


function temp(){
  for(var xx = 0; xx < result.data; xx++){
    orgLimiter.schedule( { id:sid+" - "+(xx+1)+"/"+result.data } , getNames, sid, xx)
    .catch((error)=>{
      wss.clients.forEach((ws, i) => {
        if(ws.user == "Scanner"){
          ws.send(JSON.stringify({
            type:"error",
            data:error,
            message:"There was an error getting org members, members may be missing so run "+sid+" to ensure you have every member.",
            status:0
          }));
        }
      });
    });
  }
}

function saveParam(val, id){
  sql = "UPDATE persist SET param = '"+val+"' WHERE id = "+id+";";
  con.query(sql, function(err, result, fields){
    if(err) throw err;
  })
}

function init(){
  persist(1).then(async (param) => {
    await updateQueries();
    await users(parseInt(param));
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
      queries.available = result.length;
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
  count = 0;
  console.log("Starting at #"+param+" of #"+list.length);
  async function query(username, key, i){
    await queryApi(username, key).then((result) => {
      saveParam(i, 1);
      if(result.status == 0){
        throw new Error(result.data);
      }
    })
  }
  const key = await getKey();
  for(var i = param; i < list.length; i++){
    limiter.schedule( {id:list[i].username}, query, list[i].username, key, i)
    .catch((error) => {
    });
    saved = i;
  }
}

const queryApi = function(username, key){
  return new Promise(callback => {
    /*
    var options = {
      hostname: 'api.starcitizen-api.com',
      port: 443,
      path: '/'+key+'/v1/live/user/'+escape(username),
      method: 'GET'
    }
    */
    var options = {
      hostname: 'api.mobitracker.co',
      port: 443,
      path: '/user/'+escape(username),
      method: 'GET'
    }
    const req = https.request(options, res =>{
      var body = "";
      res.on('data', d => {
        body += d;
      })
      res.on('error', error => {
        callback({ status:0, data:error});
      })
      res.on('end', function(){
        try{
          var user = JSON.parse(body);
          if(user.data == null){
            callback({status:0, data:args+" returned null. Retrying."});
          }
        }catch(err){
          var result = "Failed to parse "+username;
          callback({ status:0, data:result });
        };
        if(user){
          if(Object.size(user.data) > 0){
            cachePlayer(user.data);
            callback({ status:1 });
          }else{
            callback({ status:0, data:username+" not found." });
          }
        }else{
          console.log("User Not Found");
          callback({ status:0, data:username+" not found." });
        }
      })
    })
    req.on('error', (err) => {
      callback({ status:0, data:err});
    })
    req.end();
  });
}

var saved = 0;

function cachePlayer(user){
  if(typeof user === 'string'){
    const sql = "SELECT * FROM `CACHE players` WHERE username = '"+user+"'";
    con.query(sql, function (err, result, fields) {
      if(err) throw err;
      if(result.length > 0){
        const last = result.length-1;
        if(result[last].event != "Changed Name"){
          const sql = "INSERT INTO `CACHE players` (event, cID, username, bio, badge, organization, avatar) VALUES ( 'Changed Name', "+result[last].cID+", '"+result[last].username+"', ?, '"+result[last].badge+"', '"+result[last].organization+"', '"+result[last].avatar+"' );";
          con.query(sql, [result[last].bio], function (err, result, fields) {
            if(err) throw err;
          });
        }
      }
    });
  }else{
    var update = false;
    var eventUpdate = new Array();
    var check = { cID:0,
                  username:'',
                  badge: { src:'', title:'' },
                  organization: [],
                  avatar: ''
                };
    check.cID = parseInt(user.profile.id.substring(1));
    check.bio = JSON.stringify(user.profile.bio);
    if(!check.bio){
      check.bio = "";
    }
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
      sql = "SELECT cID, username, bio, badge, organization, avatar FROM `CACHE players` WHERE cID = "+user.profile.id.substring(1)+";";
    }else{
      check.cID = 0;
      sql = "SELECT cID, username, bio, badge, organization, avatar FROM `CACHE players` WHERE username = '"+user.profile.handle+"';";
    }
    con.query(sql, function (err, result, fields) {
      if(err) throw err;
      if(Object.size(result) > 0){
        var data = result[result.length-1];
        data.organization = JSON.parse(data.organization);
        data.organization = Object.values(data.organization);
        data.badge = JSON.parse(data.badge);
        try{
          data.bio = JSON.parse(data.bio);
        }catch{

        }
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
        if(data.cID !== check.cID){
          update = true;
          eventUpdate.push("Obtained ID");
        }
        if(data.username !== check.username){
          update = true;
          eventUpdate.push("Changed Name");
        }
        if(data.badge.title !== check.badge.title){
          update = true;
          eventUpdate.push("Badge Changed");
        }
        if(data.avatar !== check.avatar){
          update = true;
          eventUpdate.push("Avatar Changed");
        }
        if(data.bio !== check.bio){
          update = true;
          console.log({old: data.bio, new: check.bio});
          eventUpdate.push("Bio Changed");
        }
        function removeDupe(data){
          return data.filter((value, index) => data.indexOf(value) === index)
        }
        eventUpdate = removeDupe(eventUpdate);
      }else{
        check.bio = JSON.stringify(check.bio);
        check.badge = JSON.stringify(check.badge);
        check.organization = JSON.stringify(Object.assign({}, check.organization));
        const sql = "INSERT INTO `CACHE players` (event, cID, username, bio, badge, organization, avatar) VALUES ('First Entry', "+check.cID+", '"+check.username+"', ?, '"+check.badge+"', '"+check.organization+"', '"+check.avatar+"' );";
        con.query(sql, [check.bio], function (err, result, fields) {
          if(err) throw err;
        });
      }
      if(update){
        check.bio = JSON.stringify(check.bio);
        check.badge = JSON.stringify(check.badge);
        check.organization = JSON.stringify(Object.assign({}, check.organization));
        var eventString = eventUpdate.join(", ");
        const sql = "INSERT INTO `CACHE players` (event, cID, username, bio, badge, organization, avatar) VALUES ('"+eventString+"', "+check.cID+", '"+check.username+"', ?, '"+check.badge+"', '"+check.organization+"', '"+check.avatar+"');";
        con.query(sql, [check.bio], function (err, result, fields) {
          if(err) throw err;
        });
      }
    });
  }
}

function timeToComplete(){
  return new Promise(callback =>{
    sql = "SELECT username FROM `CACHE players` WHERE event = 'First Entry';";
    con.query(sql, function(err, result, fields){
      if(err) throw err;
      var max = result.length, remaining = 0, time = 0, available = queries.available;
      persist(1).then((param) =>{
        remaining = max - param;
        time = Math.ceil(remaining/available);
        console.log("Approximately "+time+" days to completion");
        callback();
      });
    })
  })
}

Object.size = function(obj) {
  var size = 0, key;
  for (key in obj) {
      if (obj.hasOwnProperty(key)) size++;
  }
  return size;
};
