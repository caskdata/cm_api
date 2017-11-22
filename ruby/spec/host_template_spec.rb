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
require_relative '../lib/endpoints/host_templates'

include ::CmApi::Endpoints::Types
include ::CmApi::Endpoints::HostTemplates

describe CmApi::Endpoints::HostTemplates do

  it 'can update' do
    resource = MockResource.new

    cluster = ApiClusterRef.new(resource, 'c1')
    tmpl = ApiHostTemplate.new(resource, 'foo')
    tmpl.instance_variable_set(:@clusterRef, cluster)

    rcgs = [
      ApiRoleConfigGroupRef.new(resource, 'rcg1'),
      ApiRoleConfigGroupRef.new(resource, 'rcg2'),
    ]

    expected = ApiHostTemplate.new(resource, 'foo', rcgs)
    # call_resource does not preserve_ro, so clusterRef not expected
    # expected.instance_variable_set(:@clusterRef, cluster)

    resource.set_expected(:put, '/clusters/c1/hostTemplates/foo', nil, expected, nil, expected.to_json_dict)
    ret = tmpl.set_role_config_groups(rcgs)
    expect(ret.roleConfigGroupRefs.length).to eq rcgs.length

  end
end
