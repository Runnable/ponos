'use strict';

var chai = require('chai');
chai.use(require('chai-as-promised'));

/* istanbul ignore next */
process.on('unhandledRejection', function (error) {
  console.error('Unhandled Promise Rejection:');
  console.error(error && error.stack || error);
  process.exit(2);
});
