package com.crypto.flink.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class SwaggerConfig {

    @Bean
    public OpenAPI cryptoTrackerOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Crypto Tracker API")
                        .description("실시간 암호화폐 데이터 분석을 위한 Apache Flink 기반 API")
                        .version("v1.0.0")
                        .contact(new Contact()
                                .name("Crypto Tracker Team")
                                .email("support@cryptotracker.com")
                                .url("https://github.com/crypto-tracker"))
                        .license(new License()
                                .name("MIT License")
                                .url("https://opensource.org/licenses/MIT")))
                .servers(List.of(
                        new Server()
                                .url("http://localhost:8082")
                                .description("Local Development Server"),
                        new Server()
                                .url("https://api.cryptotracker.com")
                                .description("Production Server")));
    }
}
