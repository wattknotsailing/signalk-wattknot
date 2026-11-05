// The gate: 100% or the build fails. Same bar as the hub and relay this plugin
// talks to. Coverage is collected from the shipped code only, so a test helper
// that is never fully exercised cannot hide a gap in the plugin itself.
module.exports = {
  testEnvironment: 'node',
  collectCoverage: true,
  collectCoverageFrom: ['index.js', 'lib/**/*.js'],
  coverageThreshold: {
    global: { lines: 100, branches: 100, functions: 100, statements: 100 }
  }
}
