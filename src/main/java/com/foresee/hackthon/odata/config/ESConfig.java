package com.foresee.hackthon.odata.config;

import com.hevelian.olastic.core.elastic.ESClient;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.util.Set;

@Configuration
public class ESConfig {


    @Bean
    Client esClient(){
        Settings settings = Settings.builder().put("cluster.name", cluster).build();
        this.client = initClient(settings,
                new InetSocketTransportAddress(InetAddress.getByName(host), port));
        initESClient();
    }

    protected Client initClient(Settings settings, TransportAddress address) {
        PreBuiltTransportClient preBuildClient = new PreBuiltTransportClient(settings);
        preBuildClient.addTransportAddress(address);
        return preBuildClient;
    }

    protected void initESClient(Client client) {
        ESClient.init(client);
    }

    @Override
    public Set<String> getIndices() {
        try {
            return client.admin().indices().stats(new IndicesStatsRequest()).actionGet()
                    .getIndices().keySet();
        } catch (NoNodeAvailableException e) {
            throw new ODataRuntimeException("Elasticsearch has no node available.", e);
        }
    }

}
