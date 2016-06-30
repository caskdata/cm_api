# encoding: UTF-8
#
# Copyright © 2012-2015 Cask Data, Inc.
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

require 'rspec'
require 'rack/test'
require 'json'

require_relative '../lib/api_client'

# Allows code to control the behavior of a resource's "invoke" method for unit testing
class MockResource < ::CmApi::Resource
  attr_accessor :version

  def initialize(version = ::CmApi::API_CURRENT_VERSION)
    super(nil)
    @_next_expect = nil
    @version = version
  end

  def base_url
    ''
  end

  # Checks the expected input data and returns the appropriate data to the caller
  def invoke(method, relpath = nil, params = nil, data = nil, headers = nil)
    exp_method, exp_path, exp_params, exp_data, exp_headers, retdata = @_next_expect
    @_next_expect = nil

    RSpec.describe MockResource do
      unless exp_method.nil?
        it 'receives expected method' do
          expect(exp_method).to eq method
        end
      end
      unless exp_path.nil?
        it 'receives expected path' do
          expect(exp_path).to eq relpath
        end
      end
      unless exp_params.nil?
        it 'receives expected params' do
          expect(exp_params).to eq params
        end
      end
      unless exp_data.nil?
        unless exp_data.is_a? String
          exp_data = Attr.new(nil, true, true).attr_to_json(exp_data, false).to_json
        end
        it 'receives expected data' do
          expect(exp_data).to eq data
        end
      end
      unless exp_headers.nil?
        it 'receives expected headers' do
          expect(exp_headers).to eq headers
        end
      end
    end
    retdata
  end

  # Ruby port note: renamed from the python version's "expect" to not conflict with RSpec

  # Sets the data to expect in the next call to invoke().
  # @param method: method to expect, or nil for any.
  # @param reqpath: request path, or nil for any.
  # @param params: query parameters, or nil for any.
  # @param data: request body, or nil for any.
  # @param headers: request headers, or nil for any.
  # @param retdata: data to return from the invoke call.
  def set_expected(method, reqpath, params = nil, data = nil, headers = nil, retdata = nil)
    @_next_expect = [method, reqpath, params, data, headers, retdata]
  end
end

# Deserializes raw JSON data into an instance of cls.

# The data is deserialized, serialized again using the class's to_json_dict()
# implementation, and deserialized again, to make sure both from_json_dict()
# and to_json_dict() are working.
def deserialize(raw_data, cls)
  instance = cls.from_json_dict(JSON.parse(raw_data), nil)
  cls.from_json_dict(instance.to_json_dict(true), nil)
end
