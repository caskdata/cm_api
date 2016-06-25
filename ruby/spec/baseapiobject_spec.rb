# #!/usr/bin/env ruby
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

require 'json'
require 'date'

require_relative '../lib/endpoints/types'

include ::CmApi::Endpoints::Types

class Child < BaseApiObject
  def _get_attributes
    { 'value' => nil }
  end
end

class Parent < BaseApiObject
  def _get_attributes
    {
      'child' => Attr.new(Child),
      'children' => Attr.new(Child),
      'date' => Attr.new(DateTime),
      'readOnly' => ROAttr.new
    }
  end
end

class Dummy < BaseApiObject
  @_ATTRIBUTES = {
    'foo' => nil,
    'bar' => nil
  }
end

describe CmApi::Endpoints::Types::BaseApiObject do
  it 'cannot set readonly or unknown properties' do
    obj = Parent.new(nil)
    obj.child = Child.new(nil)
    obj.children = []
    obj.date = Time.now.to_datetime

    # Setting read-only attribute
    expect { obj.readOnly = false }.to raise_error(RuntimeError)
    # Setting unknown attribute
    expect { obj.unknown = 'foo' }.to raise_error(NoMethodError)
  end

  it 'can serialize and deserialize' do
    json = <<-JSON
      {
        "child" : { "value" : "string1" },
        "children" : [
          { "value" : 1 },
          { "value" : "2" }
        ],
        "date" : "2013-02-12T12:17:15.831765Z",
        "readOnly" : true
      }
    JSON

    obj = deserialize(json, Parent)

    expect(obj.child).to be_an_instance_of Child
    expect(obj.child.value).to eq 'string1'
    expect(obj.children).to be_an_instance_of Array
    expect(obj.children.length).to eq 2
    expect(obj.children[0].value).to eq 1
    expect(obj.children[1].value).to eq '2'
    expect(obj.date).to be_an_instance_of DateTime
    expect(obj.date.year).to eq 2013
    expect(obj.date.month).to eq 2
    expect(obj.date.day).to eq 12
    expect(obj.date.hour).to eq 12
    expect(obj.date.minute).to eq 17
    expect(obj.date.second).to eq 15
    expect(obj.date.sec_fraction.to_f).to eq 0.831765
    expect(obj.readOnly).to eq true

    json = <<-JSON
      {
        "children" : [ ]
      }
    JSON
    obj = deserialize(json, Parent)
    expect(obj.children) == []
  end

  it 'initializes correctly' do
    obj = Parent.new(nil)
    expect(obj.instance_variable_defined?('@child')).to eq true
    expect(obj.instance_variable_defined?('@readOnly')).to eq true

    obj = Parent.new(nil, 'date' => DateTime.now)
    expect(obj.date).to be_an_instance_of DateTime

    expect { obj.readOnly = true }.to raise_error(RuntimeError)
  end

  it 'handles empty properties' do
    dummy = Dummy.new(nil)
    dummy.foo = 'foo'
    json = dummy.to_json_dict
    expect(json['foo']).to eq 'foo'
    expect(json.key?('bar')).to eq false
  end
end
