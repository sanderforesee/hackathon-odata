package com.foresee.hackthon.odata.controller;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEdmProvider;
import com.hevelian.olastic.core.api.edm.provider.MultyElasticIndexCsdlEdmProvider;
import com.hevelian.olastic.core.elastic.mappings.DefaultMetaDataProvider;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;
import com.hevelian.olastic.core.processors.impl.EntityCollectionProcessorHandler;
import com.hevelian.olastic.core.processors.impl.EntityProcessorHandler;
import com.hevelian.olastic.core.processors.impl.PrimitiveProcessorImpl;
import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.processor.ODataResponse;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHandler;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

@RestController
@RequestMapping("/odata")
public class ODataController {


    @RequestMapping(value = "odata.svc")
    public void process(HttpServletRequest req, HttpServletResponse resp) {

            OData odata = ElasticOData.newInstance();
            ServiceMetadata matadata = createServiceMetadata(req, odata, createEdmProvider());
            ODataHttpHandler handler = odata.createHandler(matadata);
            registerProcessors(handler);
            handler.process(req, resp);

    }

    protected ServiceMetadata createServiceMetadata(HttpServletRequest req, OData odata,
                                                    ElasticCsdlEdmProvider provider) {
        return odata.createServiceMetadata(provider, new ArrayList<>());
    }

    protected ElasticCsdlEdmProvider createEdmProvider() {
        return new MultyElasticIndexCsdlEdmProvider(createMetaDataProvider(), getIndices());
    }

    protected MappingMetaDataProvider createMetaDataProvider() {
        return new DefaultMetaDataProvider(getClient());
    }
    protected void registerProcessors(ODataHttpHandler handler) {
        handler.register(new PrimitiveProcessorImpl());
        handler.register(new EntityProcessorHandler());
        handler.register(new EntityCollectionProcessorHandler());
    }

}
