(function () {
    "use strict";

    const storageKey = "kubejob-dashboard-theme";
    const refreshRoot = document.querySelector("[data-kj-refresh-endpoint]");
    const baseInterval = Number(refreshRoot?.dataset.kjRefreshIntervalMs || 10000);
    let retryCount = 0;
    let timer;
    let abortController;

    function setTheme(theme) {
        document.documentElement.setAttribute("data-theme", theme);
        try { localStorage.setItem(storageKey, theme); } catch (_) { }
        const button = document.getElementById("kj-theme-toggle");
        if (button) button.textContent = theme === "dark" ? "Light mode" : "Dark mode";
    }

    function setRefreshState(isOk, message) {
        const status = document.querySelector("[data-kj-refresh-status]");
        const details = document.querySelector("[data-kj-refresh-details]");
        if (status) {
            status.classList.toggle("is-ok", isOk);
            status.classList.toggle("is-error", !isOk);
            status.textContent = isOk ? "Live" : "Retrying";
        }
        if (details) details.textContent = message;
    }

    function localDateTime(value) {
        if (!value) return "-";
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return "-";
        return date.toLocaleString(undefined, {
            year: "numeric", month: "2-digit", day: "2-digit",
            hour: "2-digit", minute: "2-digit", second: "2-digit",
            hour12: false
        });
    }

    function scheduleRefresh(delay) {
        window.clearTimeout(timer);
        if (!document.hidden && refreshRoot) timer = window.setTimeout(refresh, delay);
    }

    async function refresh() {
        if (!refreshRoot || document.hidden) return;
        if (abortController) abortController.abort();
        abortController = new AbortController();
        const timeout = window.setTimeout(() => abortController.abort(), 8000);

        try {
            const response = await fetch(refreshRoot.dataset.kjRefreshEndpoint, {
                headers: { "Accept": "application/json" },
                signal: abortController.signal
            });
            if (!response.ok) throw new Error("Request failed with " + response.status);
            const payload = await response.json();
            if (typeof window.kubeJobDashboardRender === "function") {
                window.kubeJobDashboardRender(refreshRoot, payload);
            }
            retryCount = 0;
            setRefreshState(true, "Updated " + localDateTime(payload.generatedAt || new Date()));
            scheduleRefresh(baseInterval);
        } catch (_) {
            retryCount++;
            const delay = Math.min(60000, Math.round(baseInterval * Math.pow(1.8, retryCount)));
            setRefreshState(false, "Update failed; retrying in " + Math.ceil(delay / 1000) + "s");
            scheduleRefresh(delay);
        } finally {
            window.clearTimeout(timeout);
            abortController = null;
        }
    }

    document.addEventListener("DOMContentLoaded", function () {
        const saved = document.documentElement.getAttribute("data-theme") || "light";
        setTheme(saved);
        document.getElementById("kj-theme-toggle")?.addEventListener("click", function () {
            setTheme(document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark");
        });
        if (refreshRoot) scheduleRefresh(250);
    });

    document.addEventListener("visibilitychange", function () {
        if (document.hidden) {
            window.clearTimeout(timer);
            if (abortController) abortController.abort();
        } else if (refreshRoot) {
            retryCount = 0;
            scheduleRefresh(100);
        }
    });

    window.kubeJobDashboardUtils = { safeLocalDateTime: localDateTime };
})();
