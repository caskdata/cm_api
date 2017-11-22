#!/usr/bin/env ruby
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

require 'date'

require_relative 'types'

module CmApi
  module Endpoints
    # Module for host method and types
    module HostTemplates
      include ::CmApi::Endpoints::Types

      HOST_TEMPLATES_PATH = "/clusters/%s/hostTemplates"
      HOST_TEMPLATE_PATH = "/clusters/%s/hostTemplates/%s"
      APPLY_HOST_TEMPLATE_PATH = HOST_TEMPLATE_PATH + "/commands/applyHostTemplate"

      def create_host_template(name, cluster_name)
        apitemplate = ApiHostTemplate.new(name, [])
        call_resource(method(:post), format(HOST_TEMPLATES_PATH, cluster_name), ApiHostTemplate, true, [apitemplate], nil, 3)[0]
      end

      def get_host_template(name, cluster_name)
        call_resource(method(:get), format(HOST_TEMPLATE_PATH, cluster_name, name), ApiHostTemplate, nil, nil, nil, 3)
      end

      def get_all_host_templates(cluster_name = 'default')
        call_resource(method(:get), format(HOST_TEMPLATES_PATH, cluster_name), ApiHostTemplate, true, nil, nil, 3)
      end

      def delete_host_template(name, cluster_name)
        call_resource(method(:delete), format(HOST_TEMPLATE_PATH, cluster_name, name), ApiHostTemplate, nil, nil, 3)
      end

      def update_host_template(name, cluster_name, api_host_template)
        call_resource(method(:put), format(HOST_TEMPLATE_PATH, cluster_name, name), ApiHostTemplate, nil, api_host_template, nil, 3)
      end

      def apply_host_template(name, cluster_name, host_ids, start_roles)
        host_refs = []
        host_ids.each do |host_id|
          host_refs.push(ApiHostRef.new(self, host_id))
        end

        params = {'startRoles' => start_roles }
        call_resource(method(:post), format(APPLY_HOST_TEMPLATE_PATH, cluster_name, name), ApiCommand, nil, host_refs, params, 3)
      end

      class ApiHostTemplate < BaseApiResource
        @_ATTRIBUTES = {
          'name' => nil,
          'roleConfigGroupRefs' => Attr.new(ApiRoleConfigGroupRef),
          'clusterRef' => ROAttr.new(ApiClusterRef)
        }

        def initialize(resource_root, name = nil, roleConfigGroupRefs = nil)
          super(resource_root, { name: name, roleConfigGroupRefs: roleConfigGroupRefs })
        end

        def to_s
          "<ApiHostTemplate>: #{@name} (cluster #{@clusterRef.clusterName || nil})"
        end

        def _api_version
          3
        end

        def _path
          format(HOST_TEMPLATE_PATH, @clusterRef.clusterName, @name)
        end

        def _do_update(update)
          _update(_put('', ApiHostTemplate, false, update))
          return self
        end

        def rename(new_name)
          update = self.dup
          update.name = new_name
          _do_update(update)
        end

        def set_role_config_groups(role_config_group_refs)
          update = self.dup
          update.roleConfigGroupRefs = role_config_group_refs
          _do_update(update)
        end

        def apply_host_template(host_ids, start_roles)
          # Ruby Port note: this calls a method of the same name in the module... not sure this will work
          apply_host_template(@name, @clusterRef.clusterName, host_ids, start_roles)
        end
      end
    end
  end
end
