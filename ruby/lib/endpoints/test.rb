#!/usr/bin/env ruby
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

require 'json'
require 'date'

class Attr

  DATE_FMT = "%Y-%m-%dT%H:%M:%S.%6NZ"

  def initialize(atype = nil, rw = true, is_api_list = false)
    @_atype = atype
    @_is_api_list = is_api_list
    @rw = rw
  end

  # Renamed from Cloudera's "to_json" to not conflict with json gem
  def attr_to_json(value, preserve_ro)
    if value.respond_to? 'to_json_dict'
      return value.to_json_dict(preserve_ro)
    elsif value.is_a? Hash && @_atype == ApiConfig
      return config_to_api_list(value)
    elsif value.is_a? DateTime
      return value.strftime(DATE_FMT)
    elsif value.is_a? Array # TODO: tuple support
      if @_is_api_list
        return ApiList.new(value).to_json_dict()
      else
        res = []
        value.each do |x|
          res << to_json(x, preserve_ro)
        end
        return res
      end
    else
      return value
    end
  end

  # Renamed from Cloudera's "from_json" for consistency with attr_to_json
  def attr_from_json(resource_root, data)
    return nil if data.nil?

    if @_atype == DateTime
      return DateTime.strptime(data.to_s, DATE_FMT)
    elsif @_atype == APIConfig
      # return hash for summary view, ApiList for full view. Detect from JSON data
      return {} unless data.key?('items')
      first = data['items'][0]
      return json_to_config(data, first.length == 2)
    elsif @_is_api_list
      return ApiList.from_json_dict(data, resource_root, @_atype)
    elsif data.is_a? Array
      res = []
      data.each do |x|
        res << from_json(resource_root, x)
      end
      return res
    elsif @_atype.respond_to? 'from_json_dict'
      return @_atype.from_json_dict(data, resource_root)
    else
      return data
    end
  end
end

