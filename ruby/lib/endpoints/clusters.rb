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

class CmApi
  class Endpoints
    module Clusters

      ## include types
      #def self.included klass
      #  puts "KLASS: #{klass.name}"
      #  klass.class_eval do
      #    puts "INCLUDING TYPES"
          include ::CmApi::Endpoints::Types
      #  end
      #end

      CLUSTERS_PATH = '/clusters'

      require 'pp'
      pp CmApi.constants
      pp self.constants.select {|c| self.const_get(c).is_a? Class}

      def get_cluster(name)
        return call(:get, "#{CLUSTERS_PATH}/#{name}", ApiCluster)
      end

      def get_all_clusters(view = nil)
        puts "called: get_all_clusters"
        return call(:get, CLUSTERS_PATH, ApiCluster, true, nil, view && { 'view' => view } || nil)
      end

      class ApiCluster < ::CmApi::Endpoints::Types::BaseApiResource
      #class ApiCluster < BaseApiResource
        @@_ATTRIBUTES = {
          'name' => nil,
          'displayName' => nil,
          'clusterUrl' => nil,
          'version' => nil,
          'fullVersion' => nil,
          'maintenanceMode' => ::CmApi::Endpoints::Types::ROAttr.new,
          'maintenanceOwners' => ::CmApi::Endpoints::Types::ROAttr.new
        }

        def initialize(resource_root, name = nil, version = nil, fullVersion = nil)
        #def initialize(resource_root, *args)
          #puts "INITIALIZING APICLUSTER OBJECT"
          #require 'pp'
          #puts "METHODS"
          #pp self.methods
          #puts "CLASS METHODS"
          #pp self.class.methods
          #puts "CLASS VARIABLES:"
          #pp self.class.class_variables
          
          #self.class.init(resource_root, local_variables)

          # ruby equivalent of python local()
          local_names = binding.send(:local_variables)
          local_names -= [:local_names, :locals]
          locals = local_names.reduce({}) do |acc, v|
            acc[v] = binding.eval(v.to_s) unless v == :_
            acc
          end
          puts "passing locals: #{locals}"
          super(resource_root, locals) 
        end
      end 
    end
  end
end
