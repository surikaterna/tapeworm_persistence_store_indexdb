var sqlite3 = require('sqlite3').verbose();
var indexeddbjs = require('indexeddb-js');

module.exports = function() {

	var engine    = new sqlite3.Database(':memory:');
	var scope     = indexeddbjs.makeScope('sqlite3', engine);
	return scope.indexedDB;
}

/*
module.exports = function() {
	return window.indexedDB;
}
*/