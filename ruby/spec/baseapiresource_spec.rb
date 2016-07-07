# encoding: UTF-8
#
# Copyright © 2016 Cask Data, Inc.
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

include ::CmApi::Endpoints::Types

class TestApiResource < BaseApiResource
  def _path
    ''
  end

  def return_list
    _get('return_list', ApiHostRef)
  end
end

describe BaseApiResource do
  it 'can return raw lists' do
    resource = MockResource.new
    expected = [ApiHostRef.new(resource, 'foo').to_json_dict]
    resource.set_expected(:get, '/return_list', nil, nil, nil, expected)

    ret = TestApiResource.new(resource).return_list
    expect(expected.length).to eq ret.length
    expect(ret[0]).to be_an_instance_of ApiHostRef
  end
end
