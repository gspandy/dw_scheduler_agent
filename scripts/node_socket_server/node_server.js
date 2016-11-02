// ===================================
// `tail -f` in Node.js and WebSockets
// ===================================
//
// Usage:
//
// git clone git://gist.github.com/718035.git tail.js
// cd tail.js
// git submodule update --init
//
// node server.js /path/to/your.log
//
// Connect with browser and watch changes in the logfile
//
//

//指定 node 安装的 socket.io 模块地址
var socket_model = "/usr/local/lib/node_modules/socket.io";
var signal_file_path = "/data/log/dwlogs/schedule_log/signal_file";

var http = require('http'),
io = require(socket_model),
querystring = require("querystring"),
fs = require('fs');
var url = require('url');
var util = require('util');
 
var spawn = require('child_process').spawn;

process.argv.forEach(function(val, index, array) {
  console.log(index + ': ' + val);
}); 

//var filename = process.argv[2];
var filename='';
//if (!filename) return util.puts("Usage: node <server.js> <filename>");

 
// -- Node.js Server ----------------------------------------------------------
 
server = http.createServer(function(req, res){
    var pathname = url.parse(req.url).pathname;
    var param = querystring.parse(url.parse(req.url).query)['aa'];
    var urlStr= req.url;
    filename = param;
    console.log('Request for ' + urlStr + ' received.'+'aa:'+param);
    res.writeHead(200, {'Content-Type': 'text/html', 'Access-Control-Allow-Origin':'*'})
    fs.readFile(__dirname + '/index.html', function(err, data){
    res.write(data, 'utf8');
    res.end();
});
})
server.listen(8000);
 
// -- Setup Socket.IO ---------------------------------------------------------
 
var io = io.listen(server);

io.sockets.on('connection', function (socket) {
  console.log("Socket id--------------: " + socket.id );
  var filepath ='';
  var spawn = require('child_process').spawn;
  var tail;
  socket.on('filepath', function (data) {
    console.log('filePath-----------'+data);
    filepath = data;
    if(tail){
       console.log('kill spawn-----------'+tail.pid);
       tail.kill(signal='SIGTERM');
    }
    tail = spawn("tail", ["-n1000","-f", filepath]);
    socket.emit('message',{receive:'receive filepath',socketId:socket.id});
  });

  socket.on('showlog', function (data) {
    console.log('Client connected');
    //client.send( { filename : filename } );
    socket.emit('message', {filename: filepath});

    tail.stdout.on("data", function (data) {
        console.log(data.toString('utf-8'))
        //client.send( { tail : data.toString('utf-8') } )
        socket.emit('message', {tail: data.toString('utf-8')}); 
    });
    // 当子进程退出时，检查是否有错误，同时关闭文件流
    tail.on('exit', function(code) {
        if (code != 0) {
            console.log('Failed: ' + code);
        }
    });
  });
  
  socket.on('disconnect', function() {
    if(tail){
       console.log('disconnect-----------'+tail.pid);
       console.log('disconnect kill spawn-----------'+tail.pid);
       tail.kill(signal='SIGTERM');
    }
  });

  socket.on('deleteSignalFile', function (data, callback) {
    console.log('deleteSiganlFiles:'+data);
    var singalFiles= new Array();   
    singalFiles=data.toString().split(",");      
    for (i=0;i<singalFiles.length ;i++ )   
    {   
        fs.unlink(signal_file_path+singalFiles[i],
		function(err){
		    console.log('标准错误输出：\n' + err);
                    callback(false);
		}
	);
	
    }
    
    callback(true);
  });
 

});

console.log('Server running at http://0.0.0.0:8000/, connect with a browser to see tail output');
