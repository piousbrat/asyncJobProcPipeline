package com.example.jobs.web;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Identifies the calling client: {@code X-Client-Id}, falling back to the remote address. Behind a gateway, derive it
 * from the authenticated API key instead, since the header is caller-supplied and many clients can share one IP.
 */
public final class ClientIdentity {

    public static final String CLIENT_HEADER = "X-Client-Id";
    public static final String EVENT_HEADER = "X-Event-Id";

    private ClientIdentity() {
    }

    public static String of(HttpServletRequest request) {
        String header = request.getHeader(CLIENT_HEADER);
        if (header != null && !header.isBlank()) {
            String trimmed = header.trim();
            return "client:" + (trimmed.length() > 128 ? trimmed.substring(0, 128) : trimmed);
        }
        return "ip:" + request.getRemoteAddr();
    }
}
