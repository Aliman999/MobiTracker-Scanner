'use strict';
const config  = require('./config');
const Bottleneck = require('bottleneck');
const https = require('https');
const mysql = require('mysql');
const fs = require('fs');
const schedule = require('node-schedule');
const countdown = require('countdown');
const log = require('single-line-log').stdout;
var jwt = require('jsonwebtoken');
const WebSocket = require('ws');

var list, queries = {}, sql, orgs = [], newOrgs = [];
var keyType = "Main";
var offset = 1;
var offsetMul = 3;
var speed = 1000;
var key;

const limiter = new Bottleneck({
  maxConcurrent: offset*offsetMul,
  minTime: (speed)
});


const orgScan = new Bottleneck({
  maxConcurrent: offset*offsetMul,
  minTime: (speed)
});

const orgPlayers = new Bottleneck({
  maxConcurrent: 1,
  minTime: (speed)
});

orgPlayers.on("failed", async (error, info) => {
  const id = info.options.id;
  //console.warn(`${id} failed: ${error}`);

  if (info.retryCount < 3) {
    return speed;
  }
});

orgPlayers.on("idle", function(info){
  init.orgCrawl();
});

const orgLimiter = new Bottleneck({
  maxConcurrent: 1,
  minTime: (speed)
});

orgLimiter.on("failed", async (error, info) => {
  const id = info.options.id;
  //console.warn(`${id} failed: ${error}`);

  if (info.retryCount < 3) {
    return speed;
  }
});

orgLimiter.on("idle", function(info){
  newOrgs = [];
  saveParam(0, 4);
  console.log("[SYSTEM] - Reached end of org crawl.");
  init.orgScan();
})

orgLimiter.on("done", async function(info){
});

orgScan.on("failed", async (error, info) => {
  const id = info.options.id;
  //console.warn(`${id} failed: ${error}`);

  if (info.retryCount < 3) {
    return speed;
  }else{
    console.save(info.args[0]);
  }
});

orgScan.on("done", function(info){
  console.log(info);
});

limiter.on("failed", async (error, info) => {
  const id = info.options.id;
  console.warn(`${id} failed: ${error}`);

  if (info.retryCount < 3) {
    return (offset*1000);
  }else{
    cachePlayer(info.args[0]);
  }
});

limiter.on("done", function(info){
  console.log("[PLAYER]  - #"+info.args[2]+" of #"+list.length+" | "+info.args[0]);
  socket.status.player.current = info.args[2];
});

limiter.on("idle", function(info){
  console.log("[SYSTEM]  - Reached end of player list, restarting.");
  saveParam(0, 1);
  setTimeout(() => {
    init.playerScan();
  }, 3000);
});

//limiter.on("retry", (error, info) => console.log(`Retrying ${info.options.id}`));

var db = {};

db.limiter = new Bottleneck({
  maxConcurrent: 3,
  minTime: speed
});

db.limiter.on("done", function(info){
})

db.limiter.on("failed", (error, info) => {
  const id = info.options.id;
  console.warn(`${id} failed: ${error}`);
  if (info.retryCount < 3) {
    return (offset*1000);
  }
})

db.con = mysql.createPool({
  host: config.MysqlHost,
  user: config.MysqlUsername,
  password: config.MysqlPassword,
  database: config.MysqlDatabase,
  multipleStatements: true
});

db.query = function(statement, extra = [], func, limit = false){
  if(!Array.isArray(extra)){
    func = extra;
    extra = [];
  }
  if(limit){
    db.limiter.schedule(query);
    function query(){
      db.con.query(statement, extra, func);
    }
  }else{
    db.con.query(statement, extra, func);
  }
};

db.con.getConnection((err, connection)=>{
  if (err) throw err;
  console.log("[SYSTEM]  - Connected to database");
  setInterval(() => {
    socket.send();
  }, 10000);
  init.playerScan();
  init.orgCrawl();
})



function getKey(){
  return new Promise(callback =>{
    var apiKey;
    const sql = "SELECT id, apiKey, count FROM apiKeys WHERE note like '%main%' GROUP BY id, apiKey, count ORDER BY count desc LIMIT 1";
    db.query(sql, function (err, result, fields){
      if(err) throw err;
      apiKey = result[0].apiKey;
      callback(apiKey);
    });
  })
}

function saveParam(val, id){
  sql = "UPDATE persist SET param = '"+val+"' WHERE id = "+id+";";
  db.query(sql, function(err, result, fields){
    if(err) throw err;
  })
}

var init = {};

init.playerScan = async function(){
  key = await getKey();
  persist(1).then((param) => {
    list = [];
    updateQueries().then(()=>{
      users(parseInt(param));
    })
  })
}

init.orgCrawl = async function(){
  newOrgs = [];
  sql = "SELECT DISTINCT organization->'$**.*.sid' AS org FROM `CACHE players`;";
  db.query(sql, function(err, result, fields){
    if(err) throw err;
    function onlyUnique(value, index, self) {
      return self.indexOf(value) === index;
    }
    var temp;
    result.forEach((item, i) => {
      temp = JSON.parse(item.org);
      temp.forEach((item, i) => {
        newOrgs.push(item);
      });
    });

    newOrgs = newOrgs.filter(onlyUnique);
    newOrgs.splice( newOrgs.indexOf("N/A"), 1);
    newOrgs.sort();

    console.log("[CRAWLER] - Crawling "+newOrgs.length+" Orgs");
    persist(4).then((param) => {
      socket.status.crawler = { current: parseInt(param), max:newOrgs.length };
      getOrgs.getNewOrgs(param);
    })
  })
}

init.orgScan = async function(){
  var save = 0;
  persist(3).then((param) => {
    save = param;
    getOrgs.getOrgs(param).then((result)=>{
      console.log("[SCANNER] - Scanning "+orgs.length+" cached orgs.");

      socket.status.scanner = { current: parseInt(param), max:orgs.length };

      for(var xi = param; xi < orgs.length; xi++){
        var pages = Math.ceil(orgs[xi].members/32);
        for(var xii = 0; xii < pages; xii++){
          orgScan.schedule( { id:"[SCANNER] - "+(xii+1)+" | "+orgs[xi].sid }, getNames, orgs[xi].sid, xii, xi)
          .catch((error) => {
          })
        }
      }
    })
  })

  var active;
  async function getNames(sid, page, i){
    async function query(username, key, i){
      if(active != orgs[i].sid){
        active = orgs[i].sid;
        console.log("[SCANNER] - "+orgs[i].members+" Members | "+orgs[i].sid);
        socket.status.scanner.current = i;
      }
      await queryApi(username, key).then(async (result) => {
        if(result.status == 0){
          throw new Error(result.data);
        }else{
          if(i > save){
            saveParam(i, 3);
          }
          if(i >= orgs.length){
            orgs = [];
            init.orgScan();
          }
        }
      })
    }
    await getOrgs.orgPlayers(sid, page).then((result)=>{
      if(result.status == 0){
        throw new Error(result.data);
      }else{
        result.data.forEach((item, x) => {
          orgPlayers.schedule( {id:"[SCANNER] - "+item}, query, item, key, i )
          .catch((error) => {
          });
        });
      }
    })
  }
}

function persist(id){
  return new Promise(callback => {
    sql = "SELECT param FROM persist WHERE id = "+id;
    db.query(sql, function(err, result, fields){
      if(err) throw err;

      callback(result[0].param);
    })
  })
}

function updateQueries(){
  return new Promise(callback => {
    sql = "SELECT id, count FROM apiKeys WHERE note like '%"+keyType+"%'";
    db.query(sql, function(err, result, fields){
      if(err) throw err;
      queries.data = result;
      queries.available = result.length;
      callback();
    })
  })
}

function users(param){
  sql = "SELECT username FROM `CACHE players` WHERE event = 'First Entry';";
  db.query(sql, function(err, result, fields){
    if(err) throw err;
    list = result;
    socket.status.player = { current: param, max: list.length };
    update(param);
  })
}

var getOrgs = {};

getOrgs.getNewOrgs = async function(param = 0){
  for(var i = param; i < newOrgs.length; i++){
    orgLimiter.schedule({ id:newOrgs[i] }, scan, newOrgs[i], i)
    .catch((error) => {

    })
  }
  async function scan(org, i){
    return new Promise(callback => {
      sql = "SELECT * FROM organizations WHERE sid = '"+org+"';";
      db.query(sql, function(err, sqlResult, fields){
        if(err) console.log(err);
        console.log("[CRAWLER] - #"+i+" of #"+newOrgs.length+" | "+newOrgs[i]);
        socket.status.crawler.current = i;
        getOrgs.queryOrg(org).then((result) => {
          saveParam(i, 4);
          if(sqlResult.length == 0){
            if(result.status == 1){
              result = result.data;
              sql = "INSERT INTO organizations (archetype, banner, commitment, focus, headline, href, language, logo, members, name, recruiting, roleplay, sid, url) VALUES ('"+result.archetype+"', '"+result.banner+"', '"+result.commitment+"', '"+JSON.stringify(result.focus)+"', ?, '"+result.href+"', '"+result.lang+"', '"+result.logo+"', "+result.members+", ?, "+result.recruiting+", "+result.roleplay+", '"+result.sid+"', '"+result.url+"');";
              db.query(sql, [result.headline.plaintext, result.name], function(err, sqlResult, fields){
                if(err) console.log(err.message);
                callback( { status:1, data:"", i:i } );
              })
            }else{
              callback({ status:0, data:result.data, i:i });
            }
          }else{
            if(result.status == 1){
              getOrgs.cacheOrg(result);
            }
            callback({ status:1, data:sqlResult, i:i });
          }
        })
      })
    })
  }
}

getOrgs.cacheOrg = function(orgInfo){
  orgInfo = orgInfo.data;
  if(orgInfo.headline){
    orgInfo.headline = JSON.stringify(orgInfo.headline.plaintext);
  }

  if(orgInfo.recruiting){
    orgInfo.recruiting = 1;
  }else{
    orgInfo.recruiting = 0;
  }

  if(orgInfo.roleplay){
    orgInfo.roleplay = 1;
  }else{
    orgInfo.roleplay = 0;
  }

  var events = [];
  sql = "SELECT * FROM `CACHE organizations` WHERE sid = '"+orgInfo.sid+"';";
  db.query(sql, function(err, result, fields){
    if(err) console.log(err.message);
    result = result[result.length-1];
    if(!result){
      var sql = "INSERT INTO `CACHE organizations` (event, archetype, banner, commitment, focus, headline, href, language, logo, members, name, recruiting, roleplay, sid, url) VALUES ( ?, '"+orgInfo.archetype+"', '"+orgInfo.banner+"', '"+orgInfo.commitment+"', '"+JSON.stringify(orgInfo.focus)+"', ?, '"+orgInfo.href+"', '"+orgInfo.lang+"', '"+orgInfo.logo+"', "+orgInfo.members+", ?, "+orgInfo.recruiting+", "+orgInfo.roleplay+", '"+orgInfo.sid+"', '"+orgInfo.url+"');";
      db.query(sql, ['First Entry', orgInfo.headline, orgInfo.name, orgInfo.headline, orgInfo.name], function(err, result, fields){
        if(err) console.log(err.message+"12345");
      }, true);
    }else{
      if(result.headline){
        result.headline = JSON.stringify(result.headline);
      }
      if(result.focus){
        result.focus = JSON.parse(result.focus);
      }
      if(result.archetype != orgInfo.archetype){
        events.push("Archetype Changed");
      }
      if(result.banner != orgInfo.banner){
        events.push("Banner Changed");
      }
      if(result.commitment != orgInfo.commitment){
        events.push("Commitment Changed");
      }
      if(result.focus != orgInfo.focus){
        try{
          if(result.focus.primary.name != orgInfo.focus.primary.name){
            events.push("Primary Focus Changed");
          }
          if(result.focus.secondary.name != orgInfo.focus.secondary.name){
            events.push("Secondary Focus Changed");
          }
        }catch{
          console.log(orgInfo);
          process.exit(1);
        }
      }
      if(result.headline != orgInfo.headline){
        events.push("Headline Changed");
      }
      if(result.language != orgInfo.lang){
        events.push("Language Changed");
      }
      if(result.logo != orgInfo.logo){
        events.push("Logo Changed");
      }
      if(result.members > orgInfo.members){
        events.push("Lost "+(result.members-orgInfo.members)+" members");
      }else if(result.members < orgInfo.members){
        events.push("Gained "+(orgInfo.members-result.members)+" members");
      }
      if(result.name != orgInfo.name){
        events.push("Name Changed");
      }
      if(result.recruiting > orgInfo.recruiting){
        events.push("Stopped Recruiting");
      }else if (result.recruiting < orgInfo.recruiting) {
        events.push("Started Recruiting");
      }
      if(result.roleplay != orgInfo.roleplay){
        events.push("Roleplay Changed");
      }

      function removeDupe(data){
        return data.filter((value, index) => data.indexOf(value) === index)
      }
      events = removeDupe(events);
      if(events.length > 0){
        var sql = "INSERT INTO `CACHE organizations` (event, archetype, banner, commitment, focus, headline, href, language, logo, members, name, recruiting, roleplay, sid, url) VALUES ( ?, '"+orgInfo.archetype+"', '"+orgInfo.banner+"', '"+orgInfo.commitment+"', '"+JSON.stringify(orgInfo.focus)+"', ?, '"+orgInfo.href+"', '"+orgInfo.lang+"', '"+orgInfo.logo+"', "+orgInfo.members+", ?, "+orgInfo.recruiting+", "+orgInfo.roleplay+", '"+orgInfo.sid+"', '"+orgInfo.url+"');";
        db.query(sql, [events.join(", "), orgInfo.headline, orgInfo.name, orgInfo.headline, orgInfo.name], function(err, result, fields){
          if(err) console.log(err.message+"67890");
        }, true);
      }
    }
  })
}

getOrgs.queryOrg = function(sid){
  return new Promise(callback => {
    var options = {
      hostname: 'api.dustytavern.com',
      port: 443,
      path: '/orgInfo/'+escape(sid),
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
          var org = JSON.parse(body);
          if(org.data == null){
            callback({status:0, data:sid+" returned null."});
          }
        }catch(err){
          var result = "Failed to parse "+sid;
          callback({ status:0, data:result });
        };
        if(org){
          if(Object.size(org.data) > 0){
            callback({ status:1, data:org.data });
          }else{
            callback({ status:0, data:sid+" not found." });
          }
        }else{
          callback({ status:0, data:"Server Error." });
        }
      })
    })
    req.on('error', (err) => {
      callback({ status:0, data:err});
    })
    req.end();
  });
}

getOrgs.getOrgs = function(){
  return new Promise(callback => {
    sql = "SELECT sid, members FROM `organizations`;";
    db.query(sql, function(err, result, fields){
      if(err) throw err;
      orgs = result;
      callback();
    })
  });
}

getOrgs.orgPlayers = function (sid, page){
  return new Promise(callback => {
    var options = {
      hostname: 'api.dustytavern.com',
      port: 443,
      path: '/orgMembers/'+escape(sid)+"?page="+page,
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
            callback({status:0, data:sid+" returned null."});
          }
        }catch(err){
          var result = "Failed to parse "+sid;
          callback({ status:0, data:result });
        };
        if(user){
          if(Object.size(user.data) > 0){
            var result = [];
            user.data.forEach((item, i) => {
              result.push(item.handle);
            });
            callback({ status:1, data:result });
          }else{
            callback({ status:0, data:sid+" not found." });
          }
        }else{
          callback({ status:0, data:"Server Error." });
        }
      })
    })
    req.on('error', (err) => {
      callback({ status:0, data:err});
    })
    req.end();
  });
}

async function update(param = 0){
  console.log("[SYSTEM]  - Starting at #"+param+" of #"+list.length);
  async function query(username, key, i){
    await queryApi(username, key).then((result) => {
      saveParam(i, 1);
      if(result.status == 0){
        throw new Error(result.data);
      }
    })
  }
  for(var i = param; i < list.length; i++){
    limiter.schedule( {id:list[i].username}, query, list[i].username, key, i)
    .catch((error) => {
    });
    saved = i;
  }
}

const queryApi = function(username, key){
  return new Promise(callback => {
    var options = {
      hostname: 'api.dustytavern.com',
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
    db.query(sql, function (err, result, fields) {
      if(err) throw err;
      if(result.length > 0){
        const last = result.length-1;
        if(result[last].event != "Changed Name"){
          const sql = "INSERT INTO `CACHE players` (event, cID, username, bio, badge, organization, avatar) VALUES ( 'Changed Name', "+result[last].cID+", '"+result[last].username+"', ?, '"+result[last].badge+"', '"+result[last].organization+"', '"+result[last].avatar+"' );";
          db.query(sql, [result[last].bio], function (err, result, fields) {
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
    check.bio = JSON.stringify(user.profile.bio); // DO NOT CHANGE IT IS WORKING PROPERLY. USE JSON.PARSE ON EXTRACTION.
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
    db.query(sql, function (err, result, fields) {
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
        db.query(sql, [check.bio], function (err, result, fields) {
          if(err) throw err;
        });
      }
      if(update){
        check.bio = JSON.stringify(check.bio);
        check.badge = JSON.stringify(check.badge);
        check.organization = JSON.stringify(Object.assign({}, check.organization));
        var eventString = eventUpdate.join(", ");

        const sql = "INSERT INTO `CACHE players` (event, cID, username, bio, badge, organization, avatar) VALUES ('"+eventString+"', "+check.cID+", '"+check.username+"', ?, '"+check.badge+"', '"+check.organization+"', '"+check.avatar+"');";
        db.query(sql, [check.bio], function (err, result, fields) {
          if(err) throw err;
          console.log(sql);
        });
      }
    });
  }
}

var logSave = console.save;
console.save = function(msg) {
  const date = new Date();
  const day = ("0" + date.getDate()).slice(-2);
  const month = ("0" + (date.getMonth() + 1)).slice(-2);
  const year = date.getFullYear();
  fs.appendFile('/home/ubuntu/logs/scanner.log', "["+month+"/"+day+"/"+year+" "+date.toLocaleTimeString('en-US')+"]"+" - "+msg+'\n', function(err) { if(err) {
      return trueLog(err);
    }
  });
}

Object.size = function(obj) {
  var size = 0, key;
  for (key in obj) {
      if (obj.hasOwnProperty(key)) size++;
  }
  return size;
};

//Client to API for Admin Panel.
var socket = {
  ws:null,
  init:()=>{
    socket.ws = new WebSocket("wss://ws.mobitracker.co:2599");

    socket.ws.onopen = function () {
      console.log("Connected to Internal API");
      var payload = jwt.sign({ iat: Math.floor(Date.now() / 1000) + (60 * 5), user: "Scanner" }, config.Secret, { algorithm: 'HS256' });
      var message = {
        type: "progress",
        token: payload
      };
      socket.ws.send(JSON.stringify(message));
      socket.heartbeat();
    }

    socket.ws.onerror = function (err) {
    }

    socket.ws.onclose = function () {
      console.log("Lost Connection to Internal API");
      setTimeout(socket, 3000);
    };

    socket.ws.onmessage = function (response) {
      response = JSON.parse(response.data);
      console.log(response);
    }
  },
  heartbeat:()=>{
    if (!socket.ws) return;
    if (socket.ws.readyState !== 1) return;
    socket.ws.send(JSON.stringify({ type: "ping" }));
    setTimeout(socket.heartbeat, 3000);
  },
  send:()=>{
    socket.ws.send(JSON.stringify({ type: "update", data: socket.status }));
  },
  status:{
    player:{ current:0, max:0 },
    crawler:{ current: 0, max: 0 },
    scanner:{ current: 0, max: 0 }
  }
}

socket.init();
