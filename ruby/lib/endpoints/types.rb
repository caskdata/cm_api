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

class CmApi
  class Endpoints
    module Types

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
            #return ApiList.from_json_dict(data, resource_root, @_atype)
            return ApiList.from_json_dict(self.class, data, resource_root, @_atype)
          elsif data.is_a? Array
            res = []
            data.each do |x|
              res << from_json(resource_root, x)
            end
            return res
          elsif @_atype.respond_to? 'from_json_dict'
            #return @_atype.from_json_dict(data, resource_root)
            return @_atype.from_json_dict(self.class, data, resource_root)
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
        puts "IN CALL"
        puts "  METHOD: #{method}"
        puts "  RETTYPE: #{ret_type}"
        check_api_version(self, api_version)
        if !data.nil?
          data = (Attr.new(nil, true, false).attr_to_json(data, false)).to_json
          ret = self.send(method, path, params, data)
        else
          ret = self.send method, path, params
        end

        if ret_type.nil?
          return
        elsif ret_is_list
          #return ApiList.from_json_dict(ret, self, ret_type)
          return ApiList.from_json_dict(ret, self, ret_type)
        elsif ret.is_a? Array
          res = []
          ret.each do |x|
            #res << ret_type.from_json_dict(x, self)
            res << ret_type.from_json_dict(x, self)
          end
          return res
        else
          #return ret_type.from_json_dict(ret, self)
          puts "CALLING FROM_JSON_DICT"
          puts "  implict arg: #{self.class}"
          puts "  rr: #{self}"
          return ret_type.from_json_dict(ret, self)
        end
      end

## api_client, from endpoints import clusters
# return clusters.get_cluster(self, name)
#
##cm_api.endpoints.clusters
## this is the resource_root: passing my own .get method
## resrouce_root is an instance of ApiResource
# return call(resource_root.get, "%s/%s" % (CLUSTERS_PATH, name), ApiCluster)
#
## call (cm_api.endpoints.types.call?)   (INSTANCE)
# # call method
#def call(method, path, ret_type, ret_is_list=False, data=None, params=None, api_version=1):
# ret = method(path, data=data, params=params)
# # return result
# return ret_type.from_json_dict(ret, method.im_self)
#
## BaseApiObject
#  @classmethod
#  def from_json_dict(cls, dic, resource_root):
#    from pprint import pprint
#    print "CLS"
#    pprint(cls)
#    obj = cls(resource_root)
#    obj._set_attrs(dic, allow_ro=True)
#    return obj
#
## BaseApiObject
#def __init__(self, resource_root, **attrs):



      class BaseApiObject
        @@_ATTRIBUTES = {}
        @@_WHITELIST = [ '_resource_root', '_attributes' ]

        attr_reader :_resource_root

        #def self._get_attributes()
        #  return self.class_variable_get(:@@_ATTRIBUTES)
        #end
        def _get_attributes()
          return self.class.class_variable_get(:@@_ATTRIBUTES)
        end


        # TODO: I don't think this is needed... moving this logic to initialize
        def self.init(resource_root, attrs = nil)
          puts "INIT method of class #{self} called"
          require 'pp'
          pp resource_root
          puts "vvvvvv"
          attrs.each do |k, v|
            puts "#{k} => #{v}"
          end
          pp attrs
          puts "^^^^^^"
          str_attrs = {}
          if attrs
            attrs.each do |k, v|
              unless ['self', 'resource_root'].include? k
                str_attrs[k] = v
              end
            end
          end
          initialize(resource_root, str_attrs)
        end

        def initialize(resource_root, args)
          puts "INITIALIZE method of #{self} called"
          require 'pp'
          puts "RR:"
          pp resource_root
          puts "ARGS:"
          pp args
     
          args.reject! {|x, _v| [:resource_root, :self].include? x}
          puts "FILTERED ARGS:"
          pp args 
          
          @_resource_root = resource_root

          self._get_attributes().each do |name, attr|
            #self.instance_variable_set("@#{name}", nil)
            puts "calling __setattr__ to set #{name} => nil, ignoring #{attr}"
            __setattr__(name, nil)
          end
          if args
            puts "inialize now calling _set_attrs with args: #{args}"
            _set_attrs(args, false, false)
          end
        end

      # TODO: functions should be converted to hash arguments, as they are often called using named parameters in python
        def _set_attrs(attrs, allow_ro = false, from_json = true)
          puts "_SET_ATTRS CALLED with attrs: #{attrs}"
          attrs.each do |k, v|
            puts "processing k,v: #{k} => #{v}"
            attr = _check_attr(k.to_s, allow_ro)
            if attr && from_json
              puts "checking for v"
              v = attr.from_json(@_resource_root, v)
              puts "determined v: #{v}"
              #self.instance_variable_set("@#{k}", v)
            end
            __setattr__(k, v)
          end
        end

      # TODO: make sure this works
        def __setattr__(name, val)
          unless @@_WHITELIST.include? name
            _check_attr(name.to_s, false)
          end
          self.instance_variable_set("@#{name}", val)
        end

        def _check_attr(name, allow_ro)
          unless self._get_attributes().key? name
            raise "Invalid property #{name} for class #{self.class.name}"
          end
          attr = _get_attributes()[name]
          if !allow_ro && attr && attr.respond_to?('rw') && !attr.rw
            raise "Attribute #{name} of class #{self.class.name} is read only."
          end
          return attr
        end

        def _update(api_obj)
          unless api_obj.is_a? self.class
            raise "Class #{self.class} does not derive from #{api_obj.class}; cannot update attributes."
          end

          self._get_attributes().keys.each do |name|
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
          self._get_attributes().each do |name, attr|
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
          name = self._get_attributes().keys()[0]
          value = self.instance_variable_get("@#{name}") || nil
          return "#{self.class.name}: #{name} = #{value}"
        end

        def self.from_json_dict(dic, resource_root)
          puts "FROM_JSON_DICT called"
          puts "  rr: #{resource_root}"
          puts "  self: #{self}"
          obj = self.new(resource_root)
          puts "  BACK IN FROM_JSON_DICT"
          puts "    setting attrs on #{obj}._set_attrs: #{dic}"
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

#       def self.from_json_dict(dic, resource_root)
#          puts "FROM_JSON_DICT called"
#          puts "  rr: #{resource_root}"
#          puts "  self: #{self}"
#          obj = self.new(resource_root)
#          puts "  BACK IN FROM_JSON_DICT"
#          puts "    setting attrs on #{obj}._set_attrs: #{dic}"
#          obj._set_attrs(dic, true, false)
#          return obj


#        def self.from_json_dict(dic, resource_root, member_cls = nil)
        def self.from_json_dict(cls, dic, resource_root, member_cls = nil)
          require 'pp'
          pp cls
          if member_cls.nil?
            member_cls = cls.class_variable_get(:@@_MEMBER_CLASS)
          end
          attr = Attr.new(member_cls)
          items = []

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
          end
          return ret
        end
      end

      class ApiHostRef < BaseApiObject
        @@_ATTRIBUTES = {
          'hostId' => nil
        }

        def initialize(resource_root, hostId = nil)
          init(resource_root, local_variables)
        end

        def to_str
          return "<ApiHostRef>: #{hostId}"
        end
      end
    end
  end
end