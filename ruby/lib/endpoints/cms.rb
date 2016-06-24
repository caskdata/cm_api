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

#require 'date'

require_relative 'types'
require_relative 'services'

module CmApi
  module Endpoints
    module Cms

      include ::CmApi::Endpoints::Types
      include ::CmApi::Endpoints::Services

      class ApiLicense < BaseApiObject
        @_ATTRIBUTES = {
          'owner' => ROAttr.new,
          'uuid' => ROAttr.new,
          'expiration' => ROAttr.new
        }

        def initialize(resource_root)
          super(resource_root)
        end
      end

      class ClouderaManager < BaseApiResource
        def initialize(resource_root)
          super(resource_root)
        end

        def _path()
          return '/cm'
        end

        def get_commands(view = nil)
          return _get('commands', ApiCommand, true, nil, view && { 'view' => view } || nil)
        end

        def create_mgmt_service(service_setup_info)
          return _put('service', ApiService, false, service_setup_info)
        end

        def delete_mgmt_service()
          return _delete('service', ApiService, false, nil, 6)
        end

        def get_service()
          return _get('service', ApiService)
        end

        def get_license()
          return _get('license', ApiLicense)
        end

        def get_config(view = nil)
          return _get_config('config', view)
        end

        def update_config(config)
          return _update_config('config', config)
        end

        def generate_credentials()
          return _cmd('generateCredentials')
        end

        def import_admin_credentials(username, password)
          return _cmd('importAdminCredentials', nil, {'username' => username, 'password' => password})
        end

        def inspect_hosts()
          return _cmd('inspectHosts')
        end

        def get_all_hosts_config(view = nil)
          return _get_config('allHosts/config', view)
        end

        def update_all_hosts_config(config)
          return _update_config('allHosts/config', config)
        end

        def auto_assign_roles()
          _put('service/autoAssignRoles', nil, false, nil, nil, 6)
        end

        def auto_configure()
          _put('service/autoConfigure', nil, false, nil, nil, 6)
        end

        def host_install(user_name, host_names, ssh_port = nil, password = nil,
                private_key = nil, passphrase = nil, parallel_install_count = nil,
                cm_repo_url = nil, gpg_key_custom_url = nil,
                java_install_strategy = nil, unlimited_jce = nil)

          host_install_args = {}
          host_install_args['userName'] = user_name if user_name
          host_install_args['hostNames'] = host_names if host_names
          host_install_args['sshPort'] = ssh_port if ssh_port
          host_install_args['password'] = password if password
          host_install_args['privateKey'] = private_key if private_key
          host_install_args['passphrase'] = passphrase if passphrase
          host_install_args['parallelInstallCount'] = parallel_install_count if parallel_install_count
          host_install_args['cmRepoUrl'] = cm_repo_url if cm_repo_url
          host_install_args['gpgKeyCustomUrl'] = gpg_key_custom_url if gpg_key_custom_url
          host_install_args['javaInstallStrategy'] = java_install_strategy if java_install_strategy
          host_install_args['unlimitedJCE'] = unlimited_jce if unlimited_jce

          return _cmd('hostInstall', host_install_args)
        end

        def begin_trial()
          _post('trial/begin', nil, false, nil, nil, 6)
        end

        def end_trial()
          _post('trial/end', nil, false, nil, nil, 6)
        end
      end
    end
  end
end
