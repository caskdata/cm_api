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
require_relative '../lib/endpoints/types'
require_relative '../lib/endpoints/services'
#require_relative '../lib/endpoints/clusters'

include ::CmApi::Endpoints::Types
include ::CmApi::Endpoints::Services

describe CmApi::Endpoints::Services do
  resource = MockResource.new
  service = ApiService.new(resource, 'hdfs1', 'HDFS')
  service.instance_variable_set(:@clusterRef, ApiClusterRef.new(resource, 'cluster1'))

  it 'can create hdfs tmp' do
    resource.set_expected(:post, '/clusters/cluster1/services/hdfs1/commands/hdfsCreateTmpDir', nil, nil, nil, ApiCommand.new(resource).to_json_dict())
    service.create_hdfs_tmp()
  end

  it 'test role commands' do
    args = ['role1', 'role2']
    expected = ApiBulkCommandList.new([ApiCommand.new(resource)])
    expected.instance_variable_set(:@errors, [ 'err1', 'err2' ])

    resource.set_expected(:post, '/clusters/cluster1/services/hdfs1/roleCommands/start', nil, ApiList.new(args), nil, expected.to_json_dict(true))
    ret = service.start_roles(args)
    expect(ret.length).to eq 1
    expect(ret.errors).to eq expected.errors
  end 
end
