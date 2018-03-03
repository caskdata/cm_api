# encoding: UTF-8
#
# Copyright © 2017 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require_relative 'spec_helper'
require_relative '../lib/endpoints/clusters'
require_relative '../lib/endpoints/services'
require_relative '../lib/endpoints/types'

require 'date'
require 'json'

include ::CmApi::Endpoints::Clusters
include ::CmApi::Endpoints::Services
include ::CmApi::Endpoints::Types

describe 'TestYarn' do

  it 'can get yarn applications' do
    resource = MockResource.new
    service = ApiService.new(resource, 'bar')

    time = Time.now.to_datetime

    resource.set_expected(:get, '/cm/service/yarnApplications', { 'from' => time.iso8601(6), 'to' => time.iso8601(6), 'filter' => '', 'limit' => 100, 'offset' => 0}, nil, nil, {'applications' => [], 'warnings' => []})
    resp = service.get_yarn_applications(time, time)
    expect(resp.applications.length).to eq 0
  end

  it 'can kill application' do
    resource = MockResource.new
    service = ApiService.new(resource, 'bar')
 
    resource.set_expected(:post, '/cm/service/yarnApplications/randomId/kill', nil, nil, nil, {'warning' => 'test'})
    resp = service.kill_yarn_application('randomId')
    expect(resp.warning).to eq 'test'
  end

  it 'can return attributes' do
    YARN_APPLICATION_ATTRS = <<EOF
    {
      "items": [
        {
          "name": "name",
          "type": "STRING",
          "displayName": "Name",
          "supportsHistograms": true,
          "description": "Name of the YARN application. Called 'name' in searches."
        },
        {
          "name": "user",
          "type": "STRING",
          "displayName": "User",
          "supportsHistograms": true,
          "description": "The user who ran the YARN application. Called 'user' in searches."
        },
        {
          "name": "executing",
          "type": "BOOLEAN",
          "displayName": "Executing",
          "supportsHistograms": false,
          "description": "Whether the YARN application is currently executing. Called 'executing' in searches."
        }]
    }
EOF

    resource = MockResource.new
    service = ApiService.new(resource, 'bar')

    resource.set_expected(:get, '/cm/service/yarnApplications/attributes', nil, nil, nil, JSON.parse(YARN_APPLICATION_ATTRS))
    resp = service.get_yarn_application_attributes
    expect(resp.length).to eq 3
    attr = resp[0]
    expect(attr).to be_an_instance_of ApiYarnApplicationAttribute
    expect(attr.name).to eq 'name'
    expect(attr.supportsHistograms).to eq true
  end

  it 'can collect yarn application diagnostics' do
    resource = MockResource.new
    service = ApiService.new(resource, 'bar')

    resource.set_expected(:post, '/cm/service/commands/yarnApplicationDiagnosticsCollection', nil, nil, nil, {'name' => 'YarnApplicationDiagnosticsCollection'})
    resp = service.collect_yarn_application_diagnostics('randomId-1', 'randomId-2', 'randomId-3')

    expect(resp.name).to eq 'YarnApplicationDiagnosticsCollection'
  end

  it 'can create yarn application diagnostics bundle' do
    resource = MockResource.new
    service = ApiService.new(resource, 'bar')

    resource.set_expected(:post, '/cm/service/commands/yarnApplicationDiagnosticsCollection', nil, nil, nil, {'name' => 'YarnApplicationDiagnosticsCollection' })
    resp = service.create_yarn_application_diagnostics_bundle(['randomId-1', 'randomId-2', 'randomId-3'], 'test_ticket', 'test comment')

    expect(resp.name).to eq 'YarnApplicationDiagnosticsCollection'
  end


end
