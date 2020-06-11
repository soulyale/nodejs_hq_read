///////////////////////////////////////////////////////////////////////
var  validSymbol=["xbrusd","xtiusd","brent","wti","hk50cash","usdhkd","gold","usdcnh","chi50cash","silver","us500cash","uk100cash","us100cash","usdjpy","audusd","eurusd","gbpusd","usdcad","usoilr","ukoilr","usdchf","eurcad","eurnzd","gbpcad","gbpnzd","euraud","eurchf","eurgbp","eurjpy","gbpaud","gbpchf","gbpjpy"];///添加品种 这里是唯一需要修改的地方
//////////////////////////////////////////////////////////////////////////
var fs = require('fs');
var WebSocket = require('ws');
var restify = require('restify');
var sqlite3 = require('sqlite3').verbose();
const wss = new WebSocket.Server({ port: 8080 });

wss.on('connection', function connection(ws) {
    ws.on('message', function incoming(message) {
       // console.info( message,"wss.onReceive.message");
        try{
            message=JSON.parse(message);
        }catch (e){
            console.info(e);
            ws.send(JSON.stringify(e));
        }
        if(message["ping"]){
           // ws.send(JSON.stringify({"pong":parseInt(Date.now()/1000)}));
		   ws.send(JSON.stringify({"pong":message["ping"]}));
        }
        if(message["sub"]){
            if(!ws["sub"]) ws["sub"]={};
            ws["sub"][message["sub"]]=1;
            console.info(ws["sub"],"wss onSubscribe message");
        }
    });
});

wss.broadcast = function broadcast(ch,data) {
   // console.info(ch,data,"wss.broadcast");
    wss.clients.forEach(function each(client) {
        if (client.readyState === WebSocket.OPEN) {
            if(ch){
                if(client["sub"] && client["sub"][ch]){
                    client.send(data);
                }
            }else{
                client.send(data);
            }
        }
    });
};
setInterval(function () {
    // wss.broadcast(null,JSON.stringify({"ping":parseInt(Date.now()/1000)}));
    //console.info("interval");
},5000);

var server = restify.createServer();
server.use(restify.plugins.bodyParser());
server.use(restify.plugins.queryParser());

var hqdata=[];
var DB= DB || {};

DB.tableValid=function(table){
    var validTable=["m1","m5","m15","m30","h1","h4","d1","w1","mn1"];
    if(validTable.indexOf(table) === -1){
        console.info(table+" is not in validTable array");
        return false;
    }else{
        return true;
    }
};
DB.databaseValid=function (database) {
    if(validSymbol.indexOf(database)=== -1){
        console.info(database+" is not in validSymbol array");
        return false;
    }else{
        return true;
    }
}
DB.createDatabase=function(file){
    if(!DB.databaseValid(file)) return false;//数据库文件不在允许的范围内，直接返回
    file='data/'+file+'.db';
    if(DB[file]){
        return DB[file];
    }else{
		//DB[file] = new sqlite3.Database(':memory:');
        DB[file]=new sqlite3.Database(file);
        if(!fs.existsSync(file)){
            fs.openSync(file, 'w');
            DB[file].parallelize(function () {
                DB[file].run('CREATE TABLE "M1" ("datetime"  INTEGER NOT NULL,"high"  REAL NOT NULL,"open"  REAL NOT NULL,"low"  REAL NOT NULL,' +
                    '"close"  REAL NOT NULL,"volume"  INTEGER NOT NULL,PRIMARY KEY ("datetime" ASC) ON CONFLICT REPLACE)',function (err) {
                    if(err) console.info(err,"new table m1");
                });
				DB[file].run('CREATE TABLE "M5" ("datetime"  INTEGER NOT NULL,"high"  REAL NOT NULL,"open"  REAL NOT NULL,"low"  REAL NOT NULL,' +
                    '"close"  REAL NOT NULL,"volume"  INTEGER NOT NULL,PRIMARY KEY ("datetime" ASC) ON CONFLICT REPLACE)',function (err) {
                    if(err) console.info(err,"new table m5");
                });
				DB[file].run('CREATE TABLE "M15" ("datetime"  INTEGER NOT NULL,"high"  REAL NOT NULL,"open"  REAL NOT NULL,"low"  REAL NOT NULL,' +
                    '"close"  REAL NOT NULL,"volume"  INTEGER NOT NULL,PRIMARY KEY ("datetime" ASC) ON CONFLICT REPLACE)',function (err) {
                    if(err) console.info(err,"new table m15");
                });
				DB[file].run('CREATE TABLE "M30" ("datetime"  INTEGER NOT NULL,"high"  REAL NOT NULL,"open"  REAL NOT NULL,"low"  REAL NOT NULL,' +
                    '"close"  REAL NOT NULL,"volume"  INTEGER NOT NULL,PRIMARY KEY ("datetime" ASC) ON CONFLICT REPLACE)',function (err) {
                    if(err) console.info(err,"new table m30");
                });
                DB[file].run('CREATE TABLE "H1" ("datetime"  INTEGER NOT NULL,"high"  REAL NOT NULL,"open"  REAL NOT NULL,"low"  REAL NOT NULL,' +
                    '"close"  REAL NOT NULL,"volume"  INTEGER NOT NULL,PRIMARY KEY ("datetime" ASC) ON CONFLICT REPLACE)',function (err) {
                    if(err) console.info(err,"new table h1");
                });
				DB[file].run('CREATE TABLE "H4" ("datetime"  INTEGER NOT NULL,"high"  REAL NOT NULL,"open"  REAL NOT NULL,"low"  REAL NOT NULL,' +
                    '"close"  REAL NOT NULL,"volume"  INTEGER NOT NULL,PRIMARY KEY ("datetime" ASC) ON CONFLICT REPLACE)',function (err) {
                    if(err) console.info(err,"new table h4");
                });
                DB[file].run('CREATE TABLE "D1" ("datetime"  INTEGER NOT NULL,"high"  REAL NOT NULL,"open"  REAL NOT NULL,"low"  REAL NOT NULL,' +
                    '"close"  REAL NOT NULL,"volume"  INTEGER NOT NULL,PRIMARY KEY ("datetime" ASC) ON CONFLICT REPLACE)',function (err) {
                    if(err) console.info(err,"new table d1");
                });
				DB[file].run('CREATE TABLE "W1" ("datetime"  INTEGER NOT NULL,"high"  REAL NOT NULL,"open"  REAL NOT NULL,"low"  REAL NOT NULL,' +
                    '"close"  REAL NOT NULL,"volume"  INTEGER NOT NULL,PRIMARY KEY ("datetime" ASC) ON CONFLICT REPLACE)',function (err) {
                    if(err) console.info(err,"new table w1");
                });
				DB[file].run('CREATE TABLE "MN1" ("datetime"  INTEGER NOT NULL,"high"  REAL NOT NULL,"open"  REAL NOT NULL,"low"  REAL NOT NULL,' +
                    '"close"  REAL NOT NULL,"volume"  INTEGER NOT NULL,PRIMARY KEY ("datetime" ASC) ON CONFLICT REPLACE)',function (err) {
                    if(err) console.info(err,"new table mn1");
                });
            });
        }
        return DB[file];
    }
};

server.pre(function (req,res,next) {
    //console.info(req.query);
    // res.send();
    return next();
});

//获取日期最大值
server.get('/history/querymax/:symbol/:table', function (req,res,next) {
    var db=DB.createDatabase(req.params.symbol);
    if(!db){
        res.send(200,-1);
        return;
    }
    if(DB.tableValid(req.params.table)===false) {
        res.send(200,-1);
        return;
    }
    if(db.busy){
        res.send(200,-1);
        console.info(req.getPath(),db,req.params,"db  busy,querymax return ");
        return;
    }
    db.get("select max(datetime) as datetime from "+req.params.table,function(err,row){
        if(!err){
            if(row['datetime']===null) row['datetime']=1;
            res.send(200,row['datetime']);
            console.info(req.getPath(),row,req.params,"querymax success");
        }else{
            res.send(200,-1);
            console.info(req.getPath(),err,req.params,"querymax error");
        }
    });
});


//插入bar
server.post('/history/insert/:symbol/:table',function (req,res,next) {
    console.info(req.getPath(),req.body.length,"history.insert.arr.len");
    if(req.body.length>0){
        var db=DB.createDatabase(req.params.symbol);
        if(DB.tableValid(req.params.table)===false) return;
        //  db.configure('busyTimeout', 5000);
        //如果数据库太大 应该删除一部分
        db.serialize(function () {
            db.get("select count(*) as num from "+req.params.table,function (err,row) {
                if(err) console.info(req.getPath(),err,"history.insert.delete");
                if(row.num>50000){
                    db.get("select avg(datetime) as avg,min(datetime) as min from "+req.params.table,function (err,row) {
                        if(err) console.info(req.getPath(),err,"history.insert.delete");
                        db.run("delete from "+req.params.table+" where datetime< ?" ,parseInt((row.avg+row.min)/2),function (err) {
                            if(err) console.info(req.getPath(),err);
                        });
                    });
                }
            });
            var stmt = db.prepare("INSERT INTO `"+req.params.table+"` VALUES (?,?,?,?,?,?)");
            db.busy=true;
            req.body.forEach(function (v) {
                stmt.run(v.datetime,v.high,v.open,v.low,v.close,v.volume,function (err) {
                    if(err) console.info(req.getPath(),err,db,"history.insert.run");
                });
            });
            stmt.finalize(function (err) {
                db.busy=false;
                console.info(req.getPath(),"req.params,stmt.finalize");
            });
        });
        res.send(200,req.body.length);
    }else{
        res.send();
    }
});


//实时报价
server.post('/current/quotes/:symbol',function (req,res,next) {
	if(hqdata[req.params.symbol]){
		//避免错误的价格(剔除异常的价格)
		if(hqdata[req.params.symbol]["pre_price"]){
			if(Math.abs(req.body.bid-hqdata[req.params.symbol]["pre_price"])>(req.body.bid/200)){
				res.send();
				hqdata[req.params.symbol]["pre_price"]=req.body.bid;
				return;
			}
		}
		hqdata[req.params.symbol]["pre_price"]=req.body.bid;
	}
		
	if(req.body.backup){//如果是备份服务器，只有部分功能可以应用  现在没有用
		console.info(req.body);
		if(hqdata[req.params.symbol] && Date.now()>hqdata[req.params.symbol]["mstimeout"]){
			wss.broadcast("market."+req.params.symbol+".trade.detail",
		JSON.stringify({"ch":"market."+req.params.symbol+".trade.detail","tick":{"data":[{"price":req.body.bid}]}}
		));
			wss.broadcast("market."+req.params.symbol+".kline.1min",
        JSON.stringify({"ch":"market."+req.params.symbol+".kline.1min","tick":{"data":[{"high":req.body.high,"low":req.body.low,"open":req.body.open,"close":req.body.bid,"datetime":req.body.datetime,"volume":req.body.volume}]}}
		));
			res.send(200,1);
		}else{
			res.send(200,60);
		}
		return;
	}
    
	
	wss.broadcast("market."+req.params.symbol+".trade.detail",
        JSON.stringify({"ch":"market."+req.params.symbol+".trade.detail","tick":{"data":[{"price":req.body.bid}]}}
        ));
		
    if(hqdata[req.params.symbol]==undefined) hqdata[req.params.symbol]={"datetime":req.body.datetime,"sell_price":req.body.ask,"buy_price":req.body.bid,"volume":req.body.volume};
	
	hqdata[req.params.symbol]["mstimeout"]=Date.now()+50*1000;//用于冗余服务器
	
    if(req["body"]["m1"]!=undefined){
        hqdata[req.params.symbol]["m1"]=req["body"]["m1"];
        hqdata[req.params.symbol]["m1"]["close"]=req.body.bid;
    }
	if(req["body"]["m5"]!=undefined){
        hqdata[req.params.symbol]["m5"]=req["body"]["m5"];
        hqdata[req.params.symbol]["m5"]["close"]=req.body.bid;
    }
	if(req["body"]["m15"]!=undefined){
        hqdata[req.params.symbol]["m15"]=req["body"]["m15"];
        hqdata[req.params.symbol]["m15"]["close"]=req.body.bid;
    }
	if(req["body"]["m30"]!=undefined){
        hqdata[req.params.symbol]["m30"]=req["body"]["m30"];
        hqdata[req.params.symbol]["m30"]["close"]=req.body.bid;
    }
    if(req["body"]["h1"]!=undefined){
        hqdata[req.params.symbol]["h1"]=req["body"]["h1"];
        hqdata[req.params.symbol]["h1"]["close"]=req.body.bid;
    }
	if(req["body"]["h4"]!=undefined){
        hqdata[req.params.symbol]["h4"]=req["body"]["h4"];
        hqdata[req.params.symbol]["h4"]["close"]=req.body.bid;
    }
    if(req["body"]["d1"]!=undefined){
        hqdata[req.params.symbol]["d1"]=req["body"]["d1"];
        hqdata[req.params.symbol]["d1"]["close"]=req.body.bid;
    }
	if(req["body"]["w1"]!=undefined){
        hqdata[req.params.symbol]["w1"]=req["body"]["w1"];
        hqdata[req.params.symbol]["w1"]["close"]=req.body.bid;
    }
	if(req["body"]["mn1"]!=undefined){
        hqdata[req.params.symbol]["mn1"]=req["body"]["mn1"];
        hqdata[req.params.symbol]["mn1"]["close"]=req.body.bid;
    }
    if(req["body"]["preday_close"]!=undefined){
        hqdata[req.params.symbol]["preday_close"]=req["body"]["preday_close"];
    }
	if(hqdata[req.params.symbol]["m1"]){
		hqdata[req.params.symbol]["m1"]["close"]=req.body.bid;
		wss.broadcast("market."+req.params.symbol+".kline.1min",
        JSON.stringify({"ch":"market."+req.params.symbol+".kline.1min","tick":{"data":[hqdata[req.params.symbol]["m1"]]}}
		));
	}
  //  console.info(req.getPath(),req.body);
    res.send(200,Object.keys(req.body).length);
});

server.get('/current/quotes/:symbol',function (req,res,next) {
    var reqparms=req.params.symbol.split(",");
    if(reqparms.length>0){
        var outdata=[];
        reqparms.forEach(function (val) {
            if(hqdata[val]){
				hqdata[val]['symbol']=val;
                outdata.push(hqdata[val]);
            }
        });
        res.send(200,outdata);
    }
});

/////////////暂时弃用
server.get('/history/query/:symbol/:table/:begin/:end',function (req,res,next) {
    var db=DB.createDatabase(req.params.symbol);
    if(DB.tableValid(req.params.table)===false) return;
    db.all("select * from "+ req.params.table+" where datetime>=? and datetime<=?",req.params.begin,req.params.end,function (err,row) {
        res.send(200,row);
    });


});

server.get('/market/history/kline',function (req,res,next) {
    if(req.query.period==undefined || req.query.symbol==undefined) return;
	
    if(req.query.period==="1min") {
        req.query.period="m1";
        if(hqdata[req.query.symbol]){
            var latest=hqdata[req.query.symbol]["m1"];
        }
    }
	if(req.query.period==="5min"){
        req.query.period="m5";
        if(hqdata[req.query.symbol]){
            var latest=hqdata[req.query.symbol]["m5"];
        }
    }
	if(req.query.period==="15min"){
        req.query.period="m15";
        if(hqdata[req.query.symbol]){
            var latest=hqdata[req.query.symbol]["m15"];
        }
    }
	if(req.query.period==="30min"){
        req.query.period="m30";
        if(hqdata[req.query.symbol]){
            var latest=hqdata[req.query.symbol]["m30"];
        }
    }
    if(req.query.period==="60min") {
        req.query.period="h1";
        if(hqdata[req.query.symbol]){
            var latest=hqdata[req.query.symbol]["h1"];
        }
    }
	if(req.query.period==="240min") {
        req.query.period="h4";
        if(hqdata[req.query.symbol]){
            var latest=hqdata[req.query.symbol]["h4"];
        }
    }
    if(req.query.period==="1day"){
        req.query.period="d1";
        if(hqdata[req.query.symbol]){
            var latest=hqdata[req.query.symbol]["d1"];
        }
    }
	 if(req.query.period==="1week"){
        req.query.period="w1";
        if(hqdata[req.query.symbol]){
            var latest=hqdata[req.query.symbol]["w1"];
        }
    }
	if(req.query.period==="4week"){
        req.query.period="mn1";
        if(hqdata[req.query.symbol]){
            var latest=hqdata[req.query.symbol]["mn1"];
        }
    }
	
    var db=DB.createDatabase(req.query.symbol);
	
    if(!db)return;
    if(DB.tableValid(req.query.period)===false) return;
	  console.info(req.getPath(),"query test");
    db.all("select * from "+ req.query.period+" order by datetime desc limit ?",req.query.size,function (err,row) {
		//if(err) res.send(200,err);
        if(latest && row.length>0){
            if(parseInt(latest.datetime)=== parseInt(row[0].datetime)){
                row[0]=latest;
            }
            if(parseInt(latest.datetime)> parseInt(row[0].datetime)){
                row.unshift(latest);
                row.pop();
            }
        }
        res.send(200,row);
    });
});

server.listen(80, function() {
    console.log('%s listening at %s', server.name, server.url);
});