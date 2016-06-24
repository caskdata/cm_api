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

require 'rest-client'
require 'json'

module CmApi
  # Encapsulates a resource, and provides actions to invoke on it.
  class Resource

    # @param client: A Client object.
    # @param relpath: The relative path of the resource.
    def initialize(client, relpath = '')
      @client = client
      @path = relpath.chomp('/')
      @retries = 3
      @retry_sleep = 3
    end

    def base_url
      @client.base_url
    end

    def _join_uri(relpath)
      if relpath
        ::File.join(@path, relpath)
      else
        @path
      end
    end

    # Invoke an API method.
    #   @return: Raw body or JSON dictionary (if response content type is JSON).
    def invoke(method, relpath = nil, params = nil, data = nil, headers = nil)
      # Invoke an API method
      path = _join_uri(relpath)
      resp = @client.execute(method, path, params, data, headers)

      begin
        body = resp.body
      rescue => e
        raise "Command '#{method} #{path}' failed: #{e}"
      end

      # puts "DEBUG #{method} Got response: #{body[0,32]}#{body.length > 32 ? '...' : ''}"

      # TODO: detect if response is application/json and catch json parse error
      begin
        json_hash = JSON.parse(body)
        return json_hash
      rescue
        return body
      end
    end

    # Invoke the GET method on a resource.
    #   @param relpath: Optional. A relative path to this resource's path.
    #   @param params: Key-value data.
    #   @return: A dictionary of the JSON result.
    def get(relpath = nil, params = nil)
    #  for retry in 0..@retries+1
    #     sleep(@retry_sleep) if retry
        begin
          return invoke(:get, relpath, params)
        rescue => e
          # TODO: detect timeout and retry
          raise e
        end
    #  end
    end

    # Invoke the DELETE method on a resource.
    #   @param relpath: Optional. A relative path to this resource's path.
    #   @param params: Key-value data.
    #   @return: A dictionary of the JSON result.
    def delete(relpath = nil, params = nil)
      return invoke(:delete, relpath, params)
    end

    # Invoke the POST method on a resource.
    #   @param relpath: Optional. A relative path to this resource's path.
    #   @param params: Key-value data.
    #   @param data: Optional. Body of the request.
    #   @param contenttype: Optional.
    #   @return: A dictionary of the JSON result.
    def post(relpath = nil, params = nil, data = nil, contenttype = nil)
      return invoke(:post, relpath, params, data, _make_headers(contenttype))
    end

    # Invoke the PUT method on a resource.
    #   @param relpath: Optional. A relative path to this resource's path.
    #   @param params: Key-value data.
    #   @param data: Optional. Body of the request.
    #   @param contenttype: Optional.
    #   @return: A dictionary of the JSON result.
    def put(relpath = nil, params = nil, data = nil, contenttype = nil)
      return invoke(:put, relpath, params, data, _make_headers(contenttype))
    end

    def _make_headers(contenttype = nil)
      if contenttype
        return { 'Content-Type' => contenttype }
      else
        return nil
      end
    end
  end
end
