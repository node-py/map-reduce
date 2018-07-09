const cluster = require('cluster');
const cores = require('os').cpus().length;

var simple_mapred = (data, map, reduce, callback) => {

  console.log("Hello and welcome to the world of map reduce");

  var intermediate = [];
  var groups = null;

  data.forEach((e, idx) => {
    var key = e[0];
    var value = e[1];
    var map_result = map(key, value);
    intermediate = intermediate.concat(map_result);
  });

  intermediate.sort();

  groups = intermediate.reduce((res, curr) => {
    var group = res[curr[0]] || [];
    group.push(curr[1]);
    res[curr[0]] = group;
    return res;
  }, {});

  for(k in groups){
    groups[k] = reduce(k, groups[k]);
  }

  callback(groups);
};

var cluster_map_red = (data, map, reduce, callback) => {
  if(cluster.isMaster){
    //spawn worker process to perform map task
    for(var i = 0; i<cores; i++){
      var worker = cluster.fork();
      var finished = 0;
      var intermediate = [];

      worker.on('message', (msg) => {
        if(msg.about == 'done'){
          intermediate = intermediate.concat(msg.intermediate);
        }
      });

      worker.on('exit', () => {
        finished++;
        if(finished == cores){
          intermediate.sort();
          groups = intermediate.reduce((res, curr) => {
            var group = res[curr[0]];
            if(!group || typeof group != 'object'){
              group = [];
            }
            group.push(curr[1]);
            res[curr[0]] = group;
            return res;
          }, {});
          for(var k in groups){
            groups[k] = reduce(k, groups[k]);
          }

          callback(groups);
        }
      });
    }
  }
  else{
    var data_processed = 0;
    var my_data = data[cluster.worker.id - 1];
    var intermediate = [];

    while(my_data){
      var key = my_data[0];
      var value = my_data[1];
      var groups = {};
      var map_result = map(key, value);

      intermediate = intermediate.concat(map_result);
      data_processed++;
      my_data = data[(cluster.worker.id - 1) + (data_processed) * cores]
    }

    process.send({
      from: cluster.worker.id,
      about: 'done',
      intermediate: intermediate
    });

    cluster.worker.destroy();
  }
};


module.exports = {simple_mapred, cluster_map_red};
