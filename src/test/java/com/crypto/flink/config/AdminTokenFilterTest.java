package com.crypto.flink.config;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdminTokenFilterTest {

    @Test
    void 토큰이_설정되면_변경_요청은_헤더가_맞아야_통과한다() throws Exception {
        AdminTokenFilter filter = new AdminTokenFilter("secret-1");

        MockHttpServletRequest noHeader = new MockHttpServletRequest("PUT", "/api/rules/KRW-BTC");
        MockHttpServletResponse res1 = new MockHttpServletResponse();
        filter.doFilter(noHeader, res1, new MockFilterChain());
        assertEquals(401, res1.getStatus());

        MockHttpServletRequest wrong = new MockHttpServletRequest("DELETE", "/api/rules/KRW-BTC");
        wrong.addHeader(AdminTokenFilter.HEADER, "nope");
        MockHttpServletResponse res2 = new MockHttpServletResponse();
        filter.doFilter(wrong, res2, new MockFilterChain());
        assertEquals(401, res2.getStatus());

        MockHttpServletRequest ok = new MockHttpServletRequest("PUT", "/api/rules/default");
        ok.addHeader(AdminTokenFilter.HEADER, "secret-1");
        MockHttpServletResponse res3 = new MockHttpServletResponse();
        filter.doFilter(ok, res3, new MockFilterChain());
        assertEquals(200, res3.getStatus());
    }

    @Test
    void 조회와_대시보드는_막지_않고_토큰이_비면_아무것도_막지_않는다() {
        AdminTokenFilter filter = new AdminTokenFilter("secret-1");
        assertTrue(filter.shouldNotFilter(new MockHttpServletRequest("GET", "/api/rules")));
        assertTrue(filter.shouldNotFilter(new MockHttpServletRequest("GET", "/")));
        assertTrue(filter.shouldNotFilter(new MockHttpServletRequest("GET", "/api/stream")));
        assertFalse(filter.shouldNotFilter(new MockHttpServletRequest("PUT", "/api/rules/KRW-BTC")));

        AdminTokenFilter open = new AdminTokenFilter("");
        assertTrue(open.shouldNotFilter(new MockHttpServletRequest("DELETE", "/api/rules/KRW-BTC")));
    }
}
