package com.ledgerflow.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Per-instance sliding-window limit shared by the payment routes for each IP. */
@Component
public class RateLimitFilter extends OncePerRequestFilter {
    private static final long WINDOW = TimeUnit.MINUTES.toNanos(1);
    private final Map<String, ArrayDeque<Long>> clients = new HashMap<>();
    private final int limit;
    private final LongSupplier ticker;
    private long lastCleanup;

    @Autowired
    public RateLimitFilter(@Value("${ledgerflow.rate-limit.requests-per-minute:60}") int limit) {
        this(limit, System::nanoTime);
    }

    RateLimitFilter(int limit, LongSupplier ticker) {
        if (limit <= 0) throw new IllegalArgumentException("requests-per-minute must be positive");
        this.limit = limit;
        this.ticker = ticker;
        this.lastCleanup = ticker.getAsLong();
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI().substring(request.getContextPath().length());
        return "OPTIONS".equals(request.getMethod()) ||
                !(paymentPath(path, "/v1/payments") || paymentPath(path, "/v1/orchestrated-payments"));
    }

    private boolean paymentPath(String path, String root) {
        return path.equals(root) || path.startsWith(root + "/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        long retryAfter = acquire(request.getRemoteAddr());
        if (retryAfter > 0) {
            response.setStatus(429);
            response.setHeader("Retry-After", Long.toString(retryAfter));
            response.setContentType("application/json");
            response.getWriter().write("{\"status\":429,\"error\":\"Too Many Requests\",\"message\":\"Payment API rate limit exceeded\"}");
            return;
        }
        chain.doFilter(request, response);
    }

    // Cleanup and admission share a lock; the downstream chain never holds it.
    private synchronized long acquire(String ip) {
        long now = ticker.getAsLong();
        if (now - lastCleanup >= WINDOW) {
            clients.values().removeIf(timestamps -> timestamps.isEmpty()
                    || now - timestamps.peekLast() >= WINDOW);
            lastCleanup = now;
        }
        ArrayDeque<Long> times = clients.get(ip);
        if (times != null) {
            while (!times.isEmpty() && now - times.peekFirst() >= WINDOW) times.removeFirst();
            if (times.isEmpty()) {
                clients.remove(ip);
                times = null;
            }
        }
        if (times == null) {
            times = new ArrayDeque<>();
            clients.put(ip, times);
        }
        if (times.size() >= limit) {
            long remaining = WINDOW - (now - times.peekFirst());
            return Math.max(1, (remaining + TimeUnit.SECONDS.toNanos(1) - 1) / TimeUnit.SECONDS.toNanos(1));
        }
        times.addLast(now);
        return 0;
    }
}
