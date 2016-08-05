// This is a template for a Node.js scraper on morph.io (https://morph.io)
var request = require('request')
var sqlite3 = require('sqlite3').verbose()
var osmtogeojson = require('osmtogeojson')
var replaceall = require('replaceall')
var async = require('async')
var dbutility = require('./dbutility.js')

var db = new sqlite3.Database('data.sqlite')
var timeoutInSec = 60

var countries = ['Nigeria', 'Ghana', 'South Africa']

var queryAttributes = {
  amenity: ['hospital', 'police', 'fire_station', 'school', 'university'],
  office: ['government']
}

initDatabase(runQueries)

function initDatabase (callback) {
  db.run('DROP TABLE IF EXISTS data', function () {
    callback()
  })
}

function runQueries () {
  var queryQueue = {}
  countries.forEach(function (country) {
    Object.keys(queryAttributes).map(function (queryKey, index) {
      queryAttributes[queryKey].forEach(function (queryAttribute) {
        queryQueue[dbutility.createQuery(timeoutInSec, country, queryKey, queryAttribute)] = [country, queryAttribute]
      })
    })
  })
  async.eachSeries(Object.keys(queryQueue), function (query, callback) {
    function signalingEndOfProcessing () {
      callback()
    }
    fetchPage('http://overpass-api.de/api/interpreter', query, queryQueue[query], prepareRawData, signalingEndOfProcessing)
  }, function (error) {
    db.close()
    console.log('Database closed.')
    if (error) {
      console.log('Error processing data: ' + error)
    } else {
      console.log('Done with all queries.')
    }
  })
}

function fetchPage (url, query, queryArgs, callback, signalingEndFunc) {
  request.post({'url': url, timeout: (timeoutInSec * 1000), form: {'data': query}}, function (error, response, body) {
    if (error || response.statusCode !== 200) {
      console.log('Error requesting page: ' + error + ' response code: ' +
        response.statusCode + ' body: ' + body + 'query: ' + query)
      return
    }
    callback(body, queryArgs, signalingEndFunc)
  })
}

function prepareRawData (body, queryArgs, signalingEndFunc) {
  console.log('Prepare raw data.')
  var data = manipulateData(body, queryArgs)
  prepareDatabase(data, signalingEndFunc)
}

function manipulateData (body, args) {
  var data = JSON.parse(body)
  data['country'] = args[0]
  data['building_type'] = args[1]
  data['geo_json'] = osmtogeojson(data)
  return data
}

function prepareDatabase (data, signalingEndFunc) {
  var schema = createDataSchema(data)
  var sqlStatement = dbutility.createCreateStatement(schema)
  db.run(sqlStatement, function () {
    getColumnNames(data, schema, signalingEndFunc)
  })
}

function getColumnNames (data, dataSchema, signalingEndFunc) {
  var databaseTableSchema = []
  db.each('PRAGMA table_info(data)', function (error, column) {
    if (error) {
      console.log('Error get table column names.')
    }
    databaseTableSchema.push(column.name)
  }, function () {
    console.log('Get database columns. Number of columns: ' + databaseTableSchema.length)
    alterDatabaseTable(data, dataSchema, databaseTableSchema, signalingEndFunc)
  })
}

function alterDatabaseTable (data, dataSchema, databaseTableSchema, signalingEndFunc) {
  var columns = findMissingColumns(dataSchema, databaseTableSchema)
  databaseTableSchema = updateDatabaseTableSchema(columns, databaseTableSchema)
  var sqlStatement = dbutility.createAlterStatement(columns)

  db.exec(sqlStatement, function () {
    console.log('Alter database: ' + columns.length + ' column(s) added.')
    insertDataIntoDatabase(data, dataSchema, databaseTableSchema, signalingEndFunc)
  })
}

function insertDataIntoDatabase (data, dataSchema, databaseTableSchema, signalingEndFunc) {
  var sqlStatement = dbutility.createInsertStatement(databaseTableSchema)

  Object.keys(data.elements).map(function (element) {
    var databaseObject = createDatabaseObject(data, element, dataSchema)
    insertRow(databaseObject, databaseTableSchema, sqlStatement)
  })
  var databaseObjectGeoJson = createDatabaseObjectGeoJson(data)
  insertRow(databaseObjectGeoJson, databaseTableSchema, sqlStatement)
  console.log('Insert data into database.')
  setTimeout(function () {
    signalingEndFunc()
  }, 2000)
}

function createDatabaseObjectGeoJson (data) {
  var databaseObject = {}
  databaseObject['building_type'] = data['building_type']
  databaseObject['geo_json'] = JSON.stringify(data['geo_json'])
  databaseObject['country'] = data['country']

  return databaseObject
}

function createDatabaseObject (data, element, dataSchema) {
  var databaseObject = {}
  Object.keys(data.elements[element]).map(function (key) {
    if (dataSchema.hasOwnProperty(key)) {
      databaseObject[key] = data.elements[element][key]
    }
    if (key === 'tags') {
      Object.keys(data.elements[element].tags).map(function (tagKey) {
        var newKey = replaceall(':', '_', ('tags_' + tagKey).toLowerCase())
        if (dataSchema.hasOwnProperty(newKey)) {
          databaseObject[newKey] = data.elements[element].tags[tagKey]
        }
      })
    }
  })
  databaseObject['country'] = data['country']
  return databaseObject
}

function replaceMissingValuesWithNull (dataSchema, databaseObject) {
  if (databaseObject.hasOwnProperty(dataSchema)) {
    return databaseObject[dataSchema]
  } else {
    return null
  }
}

function insertRow (databaseObject, databaseTableSchema, sqlStatement) {
  var statement = db.prepare(sqlStatement)

  var values = []

  databaseTableSchema.forEach(function (key) {
    values.push(replaceMissingValuesWithNull(key, databaseObject))
  })

  statement.run(values)
  statement.finalize()
}

function findMissingColumns (dataSchema, databaseTableSchema) {
  var keys = []

  Object.keys(dataSchema).map(function (key, index) {
    if (databaseTableSchema.indexOf(key) < 0) {
      keys.push(key)
    }
  })
  return keys
}

function updateDatabaseTableSchema (keys, tableStructure) {
  Object.keys(keys).map(function (key, index) {
    if (tableStructure.indexOf(key) < 0) {
      tableStructure.push(key)
    }
  })
  return tableStructure
}

function createDataSchema (data) {
  var dataSchema = {}

  Object.keys(data.elements).map(function (element, index) {
    Object.keys(data.elements[element]).map(function (key, index) {
      if (!dataSchema.hasOwnProperty(key)) {
        dataSchema[key] = 'TEXT'
      } else if (key === 'tags') {
        Object.keys(data.elements[element].tags).map(function (tagKey, index) {
          var newKey = replaceall(':', '_', ('tags_' + tagKey).toLowerCase())
          if (!dataSchema.hasOwnProperty(newKey)) {
            dataSchema[newKey] = 'TEXT'
          }
        })
      }
    })
  })
  dataSchema['building_type'] = 'TEXT'
  dataSchema['country'] = 'TEXT'
  dataSchema['geo_json'] = 'BLOB'

  delete dataSchema.tags
  return dataSchema
}
