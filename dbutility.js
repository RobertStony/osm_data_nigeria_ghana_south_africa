var DbUtility = function () {}

DbUtility.prototype.createQuery = function (timeout, country, queryKey, queryAttribute) {
  var query = `[out:json][timeout:'${timeout}'];` +
    `area["name"="${country}"]->.boundaryarea;` +
    '(' +
    `node(area.boundaryarea)["${queryKey}"="${queryAttribute}"];` +
    ');' +
    'out body;>;out skel qt;'
  return query
}

DbUtility.prototype.createInsertStatement = function (schema) {
  var sqlStatement = 'INSERT INTO data VALUES ('

  Object.keys(schema).map(function (key, index) {
    sqlStatement += '?, '
  })

  return sqlStatement.substring(0, sqlStatement.length - 2) + ')'
}

DbUtility.prototype.createAlterStatement = function (keys) {
  var sqlStatement = 'ALTER TABLE data ADD '

  keys.forEach(function (key) {
    sqlStatement += key + ' TEXT; ' + 'ALTER TABLE data ADD '
  })

  return sqlStatement.substring(0, sqlStatement.length - 21)
}

DbUtility.prototype.createCreateStatement = function (schema) {
  var sqlStatement = 'CREATE TABLE IF NOT EXISTS data ('

  Object.keys(schema).map(function (key, index) {
    sqlStatement += key + ' ' + schema[key] + ', '
  })

  return sqlStatement.substring(0, sqlStatement.length - 2) + ')'
}

module.exports = new DbUtility()
