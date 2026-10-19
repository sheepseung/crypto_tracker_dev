package com.crypto.flink.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * 공개 배포에서 상태를 바꾸는 API 를 지킨다. admin.token 이 설정돼 있으면 /api/** 의 PUT·POST·DELETE 는
 * X-Admin-Token 헤더가 같아야 통과한다. 조회(GET)와 대시보드는 그대로 공개다. 토큰이 비어 있으면 로컬 개발로 보고 막지 않는다.
 */
@Component
public class AdminTokenFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-Admin-Token";
    private static final Logger logger = LoggerFactory.getLogger(AdminTokenFilter.class);
    private static final Set<String> MUTATING = Set.of("PUT", "POST", "DELETE", "PATCH");

    private final String adminToken;

    public AdminTokenFilter(@Value("${admin.token:}") String adminToken) {
        this.adminToken = adminToken == null ? "" : adminToken.trim();
        if (this.adminToken.isEmpty()) {
            logger.warn("ADMIN_TOKEN 이 비어 있어 규칙 변경 API 가 누구에게나 열려 있다. 공개 배포라면 .env 에 넣어라");
        }
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return adminToken.isEmpty()
                || !MUTATING.contains(request.getMethod())
                || !request.getRequestURI().startsWith("/api/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (!matches(request.getHeader(HEADER))) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setContentType("application/json;charset=UTF-8");
            response.getWriter().write("{\"error\":\"X-Admin-Token 헤더가 필요합니다\"}");
            return;
        }
        chain.doFilter(request, response);
    }

    /** 길이 차이로 새지 않도록 상수 시간 비교. */
    boolean matches(String given) {
        if (given == null) {
            return false;
        }
        return MessageDigest.isEqual(adminToken.getBytes(StandardCharsets.UTF_8),
                given.trim().getBytes(StandardCharsets.UTF_8));
    }
}
