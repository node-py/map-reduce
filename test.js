const map_red = require('./main');

var data = [

  ['book1', 'this is the simple text from book1'],
  ['book2', 'this is the simple text from book2'],
  ['book3', 'this is not at all easy text for book3'],
  ['book4', 'I do not know what is this book all about'],
  ['book5', 'Do you understand this greate and awsome book']

];


var map = (key, value) => {
  var result = [];
  var word_map = {};
  value = value.split(' ');
  value.forEach((word) => {
    word_map[word] = (word_map[word] || 0) + 1;
  });
  for(word in word_map){
    result.push([word, word_map[word]]);
  }
  return result;
};


var reduce = (key, values) => {
  var result = 0;
  values.forEach((item) => {
    result += item;
  });
  return result;
};

var callback = (result) => {
  console.log(result);
};


//map_red(data, map, reduce, callback);
map_red.simple_mapred(data, map, reduce, callback);
map_red.cluster_map_red(data, map, reduce, callback);
