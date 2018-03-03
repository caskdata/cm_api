# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name        = 'cm-api'
  s.version     = '0.0.1'
  s.date        = '2018-01-04'
  s.summary     = 'A Ruby Cloudera Manager API Client'
  s.description = s.summary
  s.authors     = ["Cask Ops"]
  s.email       = 'ops@cask.co'
  #s.files       = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(spec/|.rubocop)}) }
  s.files       = ['README.md'] + Dir['lib/**/*.rb']
  s.homepage    = 'http://www.cask.co'
  s.license     = 'Apache-2.0'
end

