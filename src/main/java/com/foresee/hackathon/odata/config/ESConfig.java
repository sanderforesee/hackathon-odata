package com.foresee.hackathon.odata.config;

import com.hevelian.olastic.core.elastic.ESClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
public class ESConfig {


    @Bean
    Client esClient() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "docker-cluster").build();
        Client client = initClient(settings,
                new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        ESClient.init(client);

        return client;
    }

    protected Client initClient(Settings settings, TransportAddress address) {
        PreBuiltTransportClient preBuildClient = new PreBuiltTransportClient(settings);
        preBuildClient.addTransportAddress(address);
        return preBuildClient;
    }


}
