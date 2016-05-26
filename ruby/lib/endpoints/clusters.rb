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

require_relative 'types'

CLUSTERS_PATH = '/clusters'

def get_cluster(name)
  return call(:get, "#{CLUSTERS_PATH}/#{name}", ApiCluster)
end

def get_all_clusters(view = nil)
  puts "called: get_all_clusters"
  return call(:get, CLUSTERS_PATH, ApiCluster, true, nil, view && { 'view' => view } || nil)
end

class ApiCluster < BaseApiResource
  @@_ATTRIBUTES = {
    'name' => nil,
    'displayName' => nil,
    'clusterUrl' => nil,
    'version' => nil,
    'fullVersion' => nil,
    'maintenanceMode' => ROAttr.new,
    'maintenanceOwners' => ROAttr.new
  }

  def initialize(resource_root, name = nil, version = nil, fullVersion = nil)
    init(resource_root, local_variables)
  end
end 
  


__END__

# TODO: this should be Module types, especially due to 'call' method, etc

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


class ROAttr < Attr

  def initialize(atype = nil, is_api_list = false)
    Attr.new(atype, false, is_api_list)
  end
end

def check_api_version(resource_root, min_version)
  if resource_root.version < min_version
    raise "Api version #{min_version} is required but #{resource_root.version} is in use."
  end
end

def call(method, path, ret_type, ret_is_list = false, data = nil, params = nil, api_version = 1)
  # method = :post, etc
  check_api_version(self, api_version)
  if !data.nil?
    data = (Attr.new(nil, true, false).attr_to_json(data, false)).to_json
    ret = self.send(method, path, params, data)
  else
    ret = self.send(method(path, params)
  end

  if ret_type.nil?
    return
  elsif ret_is_list
    return ApiList.from_json_dict(ret, self, ret_type)
  elsif ret.is_a? Array
    res = []
    ret.each do |x|
      res << ret_type.from_json_dict(x, self)
    end
    return res
  else
    return ret_type.from_json_dict(ret, self)
  end
end


class BaseApiObject
  @@_ATTRIBUTES = {}
  @@_WHITELIST = [ '_resource_root', '_attributes' ]

  attr_reader :_resource_root

  def self._get_attributes()
    return self.class_variable_get(:@@_ATTRIBUTES)
  end

  def self.init(resource_root, attrs = nil)
    str_attrs = {}
    if attrs
      attrs.each do |k, v|
        unless ['self', 'resource_root'].include? k
          str_attrs[k] = v
        end
      end
    end
    initialize(resrouce_root, str_attrs)
  end

  def initialize(resource_root, *args)
    @_resource_root = resource_root

    self.class._get_attributes().each do |name, attr|
      #self.instance_variable_set("@#{name}", nil)
      __setattr__(name, nil)
    if args
      _set_attrs(args, false, false)
    end
  end

# TODO: functions should be converted to hash arguments, as they are often called using named parameters in python
  def _set_attrs(attrs, allow_ro = false, from_json = true)
    attrs.each do |k, v|
      attr = _check_attr(k, allow_ro)
      if attr && from_json
        v = attr.from_json(@_resource_root, v)
        #self.instance_variable_set("@#{k}", v)
        __setattr__(k, v)
      end
    end
  end

# TODO: make sure this works
  def __setattr__(name, val)
    unless @@_WHITELIST.include? name
      _check_attr(name, false)
    self.instance_variable_set("@#{name}", val)
  end

  def __check_attr(name, allow_ro)
    unless self.class._get_attributes().key? name
      raise "Invalid property #{name} for class #{self.class.name}"
    end
    attr = self.class._get_attributes()[name]
    if !allow_ro && attr && !attr.rw
      raise "Attribute #{name} of class #{self.class.name} is read only."
    end
    return attr
  end

  def _update(api_obj)
    unless api_obj.is_a? self.class
      raise "Class #{self.class} does not derive from #{api_obj.class}; cannot update attributes."
    end

    self.class._get_attributes().keys.each do |name|
      begin
        val = api_obj.instance_variable_get("@#{name}")
        self.instance_variable_set("@#{name}", val)
      rescue
        puts "ignoring failed update for attr: #{name}"
      end
    end
  end

  def to_json_dict(preserve_ro = false)
    dic = {}
    self.class._get_attributes().each do |name, attr|
      next if !preserve_ro && attr && !attr.rw
      begin
        value = self.instance_variable_get("@#{name}")
        unless value.nil?
          if attr
            dic[name] = attr.to_json(value, preserve_ro)
          else
            dic[name] = value
          end
        end
      rescue
        puts "ignoring some failed to_json_dict with #{name}"
      end
    end
    return dic
  end

  def to_str
    name = self.class._get_attributes().keys()[0]
    value = self.instance_variable_get("@#{name}") || nil
    return "#{self.class.name}: #{name} = #{value}"
  end

  def self.from_json_dict(cls, dic, resrouce_root)
    obj = Object.const_get(cls).new(resource_root)
    obj._set_attrs(dic, true, false)
    return obj
  end
end


class BaseApiResource < BaseApiObject

  def _api_version
    return 1
  end

  def _path
    raise NotImplementedError
  end

  def _require_min_api_version(version)
    actual_version = @_resource_root.version
    version = max(version, _api_version)
    if actual_version < version
      raise "API version #{version} is required but #{actual_version} is in use."
    end
  end

  def _cmd(command, data = nil, params = nil, api_version = 1)
    return _post('commands/' + command, ApiCommand, false, data, params, api_version)
  end

  def _get_config(rel_path, view, api_version = 1)
    _require_min_api_version(api_version)
    params = view && { 'view' => view} || nil
    resp = @_resource_root.get(_path + '/' + rel_path, params)
    return json_to_config(resp, view == 'full')
  end

  def _update_config(rel_path, config, api_version = 1)
    _require_min_api_version(api_version)
    resp = @_resource_root.put(_path + '/' + rel_path, nil, config_to_json(config))
    return json_to_config(resp, false)
  end

  def _delete(rel_path, ret_type, ret_is_list = false, params = nil, api_version = 1)
    return _call(:delete, rel_path, ret_type, ret_is_list, nil, params, api_version)
  end

  def _get(rel_path, ret_type, ret_is_list = false, params = nil, api_version = 1)
    return _call(:get, rel_path, ret_type, ret_is_list, nil, params, api_version)
  end

  def _post(rel_path, ret_type, ret_is_list = false, data = nil, params = nil, api_version = 1)
    return _call(:post, rel_path, ret_type, ret_is_list, data, params, api_version)
  end

  def _put(rel_path, ret_type, ret_is_list = false, data = nil, params = nil, api_version = 1)
    return _call(:put, rel_path, ret_type, ret_is_list, data, params, api_version)
  end

  def _call(method, rel_path, ret_type, ret_is_list = false, data = nil, params = nil, api_version = 1)
    path = _path
    if rel_path
      path += '/' + rel_path
    end
    return call(method, path, ret_type, ret_is_list, data, params, api_version)
  end
end

class ApiList < BaseApiObject
  LIST_KEY = 'items'

  def initialize(objects, resource_root = nil, *args)
    super(resource_root, *args)
    __setattr__('objects', objects)
  end

  def to_str
    return "<ApiList>(#{@objects.length}): [#{@objects.map { |x| x.to_str}.join(', ')}]"
  end

  def to_json_dict(preserve_ro = false)
    ret = super
    attr = Attr.new
    res = []
    @objects.each do |x|
      res << attr.to_json(x, preserve_ro)
    end
    ret[LIST_KEY] = res
    return ret
  end

  def length
    @objects.length
  end

  def each(&block)
    @objects.each(&block)
  end

  def [](i)
    @objects[i]
  end

  #TODO python __getslice equivalent

  def self.from_json_dict(cls, dic, resource_root, member_cls = nil)
    if member_cls.nil?
      member_cls = cls.class_variable_get(:@@_MEMBER_CLASS)
    end
    attr = Attr.new(member_cls)
    items = []

    if dic.key? LIST_KEY
      dic[LIST_KEY].each do |x|
        items << attr.from_json(resource_root, x)
      end
    end
    ret = Object.const_get(cls).new(items)

    # Handle if class declares custom attributes
    if cls.class_variable_get(:@@_ATTRIBUTES)
      if dic.key? LIST_KEY
        dic = dic.clone
        dic.delete(LIST_KEY)
      end
    ret._set_attrs(dic, true)
    return ret
  end
end

class ApiHostRef < BaseApiObject
  _ATTRIBUTES = {
    'hostId' => nil
  }

  def initialize(resource_root, hostId = nil)
    super(resource_root, local_variables)
  end

  def to_str
    return "<ApiHostRef>: #{hostId}"
  end
end


    

  
 





__END__

API_AUTH_REALM = 'Cloudera Manager'
API_CURRENT_VERSION = 11

class ApiException < RestException
  def initialize(error)
    super
    begin
      json_body = JSON.parse(@message)
      @message = json_body['message']
    rescue
      # ignore json parsing error
    end
  end
end

class ApiResource < Resource
  attr_accessor :version
  def initialize(server_host, server_port = nil, username = 'admin', password = 'admin', use_tls = false, version = API_CURRENT_VERSION)
    @version = version
    protocol = use_tls ? 'https' : 'http'
    if server_port.nil?
      server_port = use_tls ? 7183 : 7180
    end
    base_url = "#{protocol}://#{server_host}:#{server_port}/api/v#{version}"

    client = HttpClient.new(base_url, exc_class=ApiException) 
    client.set_basic_auth(username, password, API_AUTH_REALM)
    client.headers = { :'content-type' => 'application/json' }
    super(client)
  end

  # Cluster ops

  def get_cloudera_manager
  end

  def create_cluster(name, version = nil, fullVersion = nil)
  end

  def delete_cluster(name)
  end

  def get_all_clusters(view = nil)
  end

  def get_cluster(name)
  end
end
