var changesStream = require('changes-stream')
var runParallel = require('run-parallel')
var maxSatisfying = require('semver').maxSatisfying
var normalize = require('normalize-registry-metadata')
var pressureStream = require('pressure-stream')
var pump = require('pump')
var tarStream = require('tar-stream')
var zlib = require('zlib')

module.exports = Longshore

function Longshore (levelup, blobStore, fromSequence) {
  if (fromSequence !== undefined && !validSequence(fromSequence)) {
    throw new Error('invalid sequence number')
  }
  if (!(this instanceof Longshore)) {
    return new Longshore(levelup, blobStore, fromSequence)
  }
  this._fromSequence = fromSequence || 0
  this._levelup = levelup
  this._blobStore = blobStore
}

var prototype = Longshore.prototype

prototype.start = function () {
  var self = this

  var pressure = self._pressure =
  pressureStream(function (change, next) {
    self._onChange(change, function (error, data) {
      if (error) return next(error)
      self._setSequence(change.seq, next)
    })
  }, {high: 1, max: 1, low: 1})

  var changes = self._changes = changesStream({
    db: 'https://replicate.npmjs.com',
    include_docs: true,
    since: self._fromSequence
  })

  pump(changes, pressure)
  .on('error', function (error) {
    self.emit('error', error)
  })
}

prototype.stop = function () {
  this._changes.destroy()
}

prototype._onChange = function (change, done) {
  var self = this
  var sequence = change.seq
  var doc = change.doc
  if (!doc.name || !doc.versions) {
    self._setSequence(sequence, done)
  } else { // Registry publish.
    doc = normalize(doc)
    var packageName = doc.name
    var versions = doc.versions
    var levelBatch = []
    var prepackageJobs = []
    Object.keys(versions)
    .forEach(function (packageVersion) {
      var data = versions[packageVersion]
      var dependencies = data.dependencies
      var key = encodeKey('dependencies', packageName, packageVersion)
      levelBatch.push({
        type: 'put',
        key: key,
        value: JSON.stringify(dependencies)
      })
      prepackageJobs.push(
        self._prepackage.bind(
          self,
          packageVersion,
          sequence,
          dependencies,
          done
        )
      )
    })
    runParallel(
      [
        function writeBatchToLevelUP (done) {
          self._levelup.batch(levelBatch, done)
        },
        function prepackage (done) {
          runParallel(prepackageJobs, done)
        }
      ],
      function (error) {
        done(error)
      }
    )
  }
}

var SEQUENCE_KEY = 'sequence'

prototype._setSequence = function (sequence, callback) {
  this._levelup.put(SEQUENCE_KEY, sequence, callback)
}

function encodeKey (/* variadic */) {
  return Array.prototype.slice.call(arguments)
  .map(encodeURIComponent)
  .join('/')
}

function validSequence (argument) {
  return Number.isInteger(argument) && argument > 0
}
