Welcome to Cloudera Manager API Client!

Ruby Client
=============
The ruby source is in the `ruby` directory. The Ruby client comes with a 
`cm-api` Ruby client module, and examples on performing certain Hadoop cluster 
administrative tasks using the Ruby client.

*Note, this has been ported directly from the Cloudera Python client. Effort has
been made to keep the code structurally as similar to the Python source as
possible, which in some cases goes against Ruby best practices. This is to ensure
initial functionality and coverage. The plan is to eventually restructure and 
optimize for Ruby in a later release.*

Getting Started
---------------
Here is a short snippet on using the `cm-api` Ruby client:

    irb(main):001:0> require 'cm-api'
    => true
    irb(main):002:0> api = CmApi::ApiResource.new('rhel62-1', 7180, 'admin', 'admin')
    => #<CmApi::ApiResource:0x007fe44c53b000 @version=12, @client=#<CmApi::HttpClient:0x007fe44c53af38 @base_url="http://rhel62-1.ent.cloudera.com:7180/api/v12", @exc_class=CmApi::ApiException, @headers={:"content-type"=>"application/json"}, @user="admin", @password="admin">, @path="", @retries=3, @retry_sleep=3>
    irb(main):003:0> api.get_all_hosts.each do |h|
    irb(main):004:1*   puts h.hostname
    irb(main):005:1> end
    rhel62-2.ent.cloudera.com
    rhel62-4.ent.cloudera.com
    rhel62-3.ent.cloudera.com
    rhel62-1.ent.cloudera.com
    irb(main):007:0> 

Another example: getting all the services in a cluster:

    irb(main):008:0> api.get_all_clusters.each do |c|
    irb(main):009:1*   puts c.name
    irb(main):010:1> end
    Cluster 1 - CDH4
    irb(main):011:0> api.get_cluster('Cluster 1 - CDH4').get_all_services.each do |s|
    irb(main):012:1*   puts s.name
    irb(main):013:1> end
    hdfs1
    mapreduce1
    zookeeper1
    hbase1
    oozie1
    yarn1
    hue1
    irb(main):014:0>

Example Scripts
---------------
Coming soon
