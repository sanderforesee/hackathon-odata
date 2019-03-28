package com.foresee.hackathon.odata.controller;

import com.hevelian.olastic.core.ElasticOData;
import com.hevelian.olastic.core.api.edm.provider.ElasticCsdlEdmProvider;
import com.hevelian.olastic.core.api.edm.provider.MultyElasticIndexCsdlEdmProvider;
import com.hevelian.olastic.core.elastic.mappings.DefaultMetaDataProvider;
import com.hevelian.olastic.core.elastic.mappings.MappingMetaDataProvider;
import com.hevelian.olastic.core.processors.impl.EntityCollectionProcessorHandler;
import com.hevelian.olastic.core.processors.impl.EntityProcessorHandler;
import com.hevelian.olastic.core.processors.impl.PrimitiveProcessorImpl;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.http.HttpMethod;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.deserializer.DeserializerException;
import org.apache.olingo.server.core.ODataHandlerException;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


@RestController
public class ODataController {

    @Autowired
    Client client;

    @RequestMapping("/**")
    public void process(HttpServletRequest req, HttpServletResponse resp) throws IOException, ODataLibraryException {
            String serveletPath = req.getServletPath();
            String contextPath  =req.getContextPath();
            OData odata = ElasticOData.newInstance();
            ServiceMetadata matadata = createServiceMetadata(req, odata, createEdmProvider());
            ODataHttpHandler handler = odata.createHandler(matadata);

            registerProcessors(handler);
            ODataRequest oDataRequest = new ODataRequest();
            fillODataRequest(oDataRequest,req,0);
            ODataResponse oDataResponse = handler.process(oDataRequest);

            buildResponse(resp,oDataResponse);

//              resp.getWriter().println("Test!");

    }


    protected ServiceMetadata createServiceMetadata(HttpServletRequest req, OData odata,
                                                    ElasticCsdlEdmProvider provider) {
        return odata.createServiceMetadata(provider, new ArrayList<>());
    }

    protected ElasticCsdlEdmProvider createEdmProvider() {
        Set<String> indices = getIndices(client);
        return new MultyElasticIndexCsdlEdmProvider(createMetaDataProvider(),indices);
    }

    protected MappingMetaDataProvider createMetaDataProvider() {
        return new DefaultMetaDataProvider(client);
    }
    protected void registerProcessors(ODataHttpHandler handler) {
        handler.register(new PrimitiveProcessorImpl());
        handler.register(new EntityProcessorHandler());
        handler.register(new EntityCollectionProcessorHandler());
    }

    public Set<String> getIndices(Client client) {
        try {
            return client.admin().indices().stats(new IndicesStatsRequest()).actionGet()
                    .getIndices().keySet().stream().filter(idx-> !idx.startsWith(".")).collect(Collectors.toSet());
        } catch (NoNodeAvailableException e) {
            throw new ODataRuntimeException("Elasticsearch has no node available.", e);
        }
    }

    private ODataRequest fillODataRequest(ODataRequest odRequest, HttpServletRequest httpRequest, int split) throws ODataLibraryException {

        ODataRequest var6;
        try {
            odRequest.setBody(httpRequest.getInputStream());
            odRequest.setProtocol(httpRequest.getProtocol());
            odRequest.setMethod(extractMethod(httpRequest));
            copyHeaders(odRequest, httpRequest);
            fillUriInformation(odRequest, httpRequest, split);
            var6 = odRequest;
        } catch (IOException var10) {
            throw new DeserializerException("An I/O exception occurred.", var10, DeserializerException.MessageKeys.IO_EXCEPTION, new String[0]);
        }
        return var6;
    }

    static HttpMethod extractMethod(HttpServletRequest httpRequest) throws ODataLibraryException {
        HttpMethod httpRequestMethod;
        try {
            httpRequestMethod = HttpMethod.valueOf(httpRequest.getMethod());
        } catch (IllegalArgumentException var5) {
            throw new ODataHandlerException("HTTP method not allowed" + httpRequest.getMethod(), var5, org.apache.olingo.server.core.ODataHandlerException.MessageKeys.HTTP_METHOD_NOT_ALLOWED, new String[]{httpRequest.getMethod()});
        }

        try {
            if (httpRequestMethod == HttpMethod.POST) {
                String xHttpMethod = httpRequest.getHeader("X-HTTP-Method");
                String xHttpMethodOverride = httpRequest.getHeader("X-HTTP-Method-Override");
                if (xHttpMethod == null && xHttpMethodOverride == null) {
                    return httpRequestMethod;
                } else if (xHttpMethod == null) {
                    return HttpMethod.valueOf(xHttpMethodOverride);
                } else if (xHttpMethodOverride == null) {
                    return HttpMethod.valueOf(xHttpMethod);
                } else if (!xHttpMethod.equalsIgnoreCase(xHttpMethodOverride)) {
                    throw new ODataHandlerException("Ambiguous X-HTTP-Methods", org.apache.olingo.server.core.ODataHandlerException.MessageKeys.AMBIGUOUS_XHTTP_METHOD, new String[]{xHttpMethod, xHttpMethodOverride});
                } else {
                    return HttpMethod.valueOf(xHttpMethod);
                }
            } else {
                return httpRequestMethod;
            }
        } catch (IllegalArgumentException var4) {
            throw new ODataHandlerException("Invalid HTTP method" + httpRequest.getMethod(), var4, org.apache.olingo.server.core.ODataHandlerException.MessageKeys.INVALID_HTTP_METHOD, new String[]{httpRequest.getMethod()});
        }
    }

    static void copyHeaders(ODataRequest odRequest, HttpServletRequest req) {
        Enumeration headerNames = req.getHeaderNames();

        while(headerNames.hasMoreElements()) {
            String headerName = (String)headerNames.nextElement();
            List<String> headerValues = Collections.list(req.getHeaders(headerName));
            odRequest.addHeader(headerName, headerValues);
        }

    }

    static void fillUriInformation(ODataRequest odRequest, HttpServletRequest httpRequest, int split) {
        String rawRequestUri = httpRequest.getRequestURL().toString();
        String rawODataPath;
        int beginIndex;
        if (!"".equals(httpRequest.getServletPath())) {
            beginIndex = rawRequestUri.indexOf(httpRequest.getContextPath()) + httpRequest.getServletPath().length();
            rawODataPath = rawRequestUri.substring(beginIndex);
        } else if (!"".equals(httpRequest.getContextPath())) {
            beginIndex = rawRequestUri.indexOf(httpRequest.getContextPath()) + httpRequest.getContextPath().length();
            rawODataPath = rawRequestUri.substring(beginIndex);
        } else {
            rawODataPath = httpRequest.getRequestURI();
        }

        String rawServiceResolutionUri = null;
        if (split > 0) {
            rawServiceResolutionUri = rawODataPath;

            int end;
            for(end = 0; end < split; ++end) {
                int index = rawODataPath.indexOf(47, 1);
                if (-1 == index) {
                    rawODataPath = "";
                    break;
                }

                rawODataPath = rawODataPath.substring(index);
            }

            end = rawODataPath.length() - rawODataPath.length();
            rawServiceResolutionUri = rawServiceResolutionUri.substring(0, end);
        }

        String rawBaseUri = rawRequestUri.substring(0, rawRequestUri.length() - rawODataPath.length());
        odRequest.setRawQueryPath(httpRequest.getQueryString());
        odRequest.setRawRequestUri(rawRequestUri + (httpRequest.getQueryString() == null ? "" : "?" + httpRequest.getQueryString()));
        odRequest.setRawODataPath(rawODataPath);
        odRequest.setRawBaseUri(rawBaseUri);
        odRequest.setRawServiceResolutionUri(rawServiceResolutionUri);
    }

    static HttpServletResponse buildResponse(HttpServletResponse httpResponse, ODataResponse oDataResponse) {

        try {
            /* Obtain oData response as string from oData response content stream */
            String response = StreamUtils.copyToString(oDataResponse.getContent(), Charset.forName("utf-8"));

            /* Add the odata version  */
            httpResponse.setStatus(oDataResponse.getStatusCode());
            Iterator var2 = oDataResponse.getAllHeaders().entrySet().iterator();

            while(var2.hasNext()) {
                Map.Entry<String, List<String>> entry = (Map.Entry)var2.next();
                Iterator var4 = ((List)entry.getValue()).iterator();

                while(var4.hasNext()) {
                    String headerValue = (String)var4.next();
                    httpResponse.addHeader((String)entry.getKey(), headerValue);
                }
            }

            httpResponse.getWriter().print(response);
        } finally {
            return httpResponse;
        }
    }
}
