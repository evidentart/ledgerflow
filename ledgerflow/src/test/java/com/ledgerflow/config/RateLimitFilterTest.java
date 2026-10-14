package com.ledgerflow.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimitFilterTest {
    private MockHttpServletResponse request(RateLimitFilter filter, String ip, String path) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", path);
        request.setRemoteAddr(ip);
        request.addHeader("X-Forwarded-For", "forged-client");
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, (req, res) -> ((MockHttpServletResponse) res).setStatus(204));
        return response;
    }

    @Test
    void allowsLimitThenRejectsWithRetryAfter() throws Exception {
        RateLimitFilter filter = new RateLimitFilter(2);
        assertThat(request(filter, "192.0.2.1", "/v1/payments").getStatus()).isEqualTo(204);
        assertThat(request(filter, "192.0.2.1", "/v1/payments/id/refund").getStatus()).isEqualTo(204);
        MockHttpServletResponse rejected = request(filter, "192.0.2.1", "/v1/orchestrated-payments");
        assertThat(rejected.getStatus()).isEqualTo(429);
        assertThat(Long.parseLong(rejected.getHeader("Retry-After"))).isBetween(1L, 60L);
        assertThat(rejected.getContentType()).isEqualTo("application/json");
    }

    @Test
    void separateRemoteAddressesHaveSeparateQuotas() throws Exception {
        RateLimitFilter filter = new RateLimitFilter(1);
        assertThat(request(filter, "192.0.2.1", "/v1/payments").getStatus()).isEqualTo(204);
        assertThat(request(filter, "192.0.2.1", "/v1/payments").getStatus()).isEqualTo(429);
        assertThat(request(filter, "192.0.2.2", "/v1/payments").getStatus()).isEqualTo(204);
    }

    @Test
    void honorsConfiguredLimit() {
        new ApplicationContextRunner().withUserConfiguration(RateLimitFilter.class)
                .withPropertyValues("ledgerflow.rate-limit.requests-per-minute=3")
                .run(context -> {
                    RateLimitFilter filter = context.getBean(RateLimitFilter.class);
                    for (int i = 0; i < 3; i++) {
                        assertThat(request(filter, "192.0.2.1", "/v1/payments").getStatus()).isEqualTo(204);
                    }
                    assertThat(request(filter, "192.0.2.1", "/v1/payments").getStatus()).isEqualTo(429);
                });
    }

    @Test
    void quotaRecoversAtWindowBoundary() throws Exception {
        AtomicLong time = new AtomicLong();
        RateLimitFilter filter = new RateLimitFilter(1, time::get);
        request(filter, "192.0.2.1", "/v1/payments");
        time.set(TimeUnit.SECONDS.toNanos(59));
        assertThat(request(filter, "192.0.2.1", "/v1/payments").getHeader("Retry-After")).isEqualTo("1");
        time.set(TimeUnit.SECONDS.toNanos(60));
        assertThat(request(filter, "192.0.2.1", "/v1/payments").getStatus()).isEqualTo(204);
    }

    @Test
    void periodicallyRemovesStaleClientsWithoutRemovingActiveClients() throws Exception {
        AtomicLong time = new AtomicLong();
        RateLimitFilter filter = new RateLimitFilter(2, time::get);
        request(filter, "192.0.2.1", "/v1/payments");
        time.set(TimeUnit.SECONDS.toNanos(30));
        request(filter, "192.0.2.2", "/v1/payments");

        // Inspect state only in the test; no production accessor is needed.
        Map<?, ?> clients = (Map<?, ?>) ReflectionTestUtils.getField(filter, "clients");
        assertThat(clients.containsKey("192.0.2.1")).isTrue();
        time.set(TimeUnit.SECONDS.toNanos(60));
        request(filter, "192.0.2.3", "/v1/payments");
        assertThat(clients.containsKey("192.0.2.1")).isFalse();
        assertThat(clients.containsKey("192.0.2.2")).isTrue();

        time.set(TimeUnit.SECONDS.toNanos(90));
        request(filter, "192.0.2.3", "/v1/payments");
        assertThat(clients.containsKey("192.0.2.2")).isTrue();
        time.set(TimeUnit.SECONDS.toNanos(120));
        request(filter, "192.0.2.3", "/v1/payments");
        assertThat(clients.containsKey("192.0.2.2")).isFalse();
    }

    @Test
    void concurrentRequestsCannotExceedQuota() throws Exception {
        RateLimitFilter filter = new RateLimitFilter(5);
        try (var executor = Executors.newFixedThreadPool(8)) {
            var tasks = new ArrayList<Callable<Integer>>();
            for (int i = 0; i < 40; i++) {
                tasks.add(() -> request(filter, "192.0.2.1", "/v1/payments").getStatus());
            }
            int accepted = 0;
            for (var result : executor.invokeAll(tasks)) {
                int status = result.get();
                assertThat(status).isIn(204, 429);
                if (status == 204) accepted++;
            }
            assertThat(accepted).isEqualTo(5);
        }
    }

    @Test
    void unrelatedPathsAreNotLimited() throws Exception {
        RateLimitFilter filter = new RateLimitFilter(1);
        request(filter, "192.0.2.1", "/v1/payments");
        for (String path : new String[]{"/actuator/health", "/v1/accounts", "/v1/payments-other"}) {
            assertThat(request(filter, "192.0.2.1", path).getStatus()).isEqualTo(204);
        }
    }
}
