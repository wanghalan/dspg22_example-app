'use strict'
const DataHandler = require('../data_handler.min.js'),
  data = new DataHandler(require('../settings.json'), void 0, {
    data: require('../data.json'),
  })
module.exports.handler = async function (event) {
  return data.export(event.queryStringParameters)
}
