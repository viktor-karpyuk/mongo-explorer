package com.kubrik.mex.k8s.portforward;

import com.kubrik.mex.k8s.model.PortForwardTarget;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1EndpointAddress;
import io.kubernetes.client.openapi.models.V1EndpointSubset;
import io.kubernetes.client.openapi.models.V1Endpoints;
import io.kubernetes.client.openapi.models.V1ObjectReference;

import java.util.List;
import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-C2 — Resolves a {@link PortForwardTarget} (Service
 * or Pod) down to a pod name the port-forward API can address.
 *
 * <p>For a Service target: reads the Endpoints object of the same
 * name and picks the first address in {@code subsets[].addresses}
 * (ready addresses only; {@code notReadyAddresses} is excluded —
 * we don't want to forward into a booting or terminating pod).</p>
 *
 * <p>For a Pod target: returns the pod name as-is with no API
 * call.</p>
 */
public final class PodResolver {

    private final ApiClient client;

    public PodResolver(ApiClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    public String resolvePod(PortForwardTarget target) throws ApiException {
        if (target.pod().isPresent()) return target.pod().get();
        return resolveServicePod(target.namespace(), target.service().orElseThrow());
    }

    String resolveServicePod(String namespace, String serviceName) throws ApiException {
        V1Endpoints eps = new CoreV1Api(client)
                .readNamespacedEndpoints(serviceName, namespace).execute();
        if (eps == null || eps.getSubsets() == null) {
            throw noReadyPod(namespace, serviceName);
        }
        for (V1EndpointSubset subset : eps.getSubsets()) {
            List<V1EndpointAddress> ready = subset.getAddresses();
            if (ready == null) continue;
            for (V1EndpointAddress addr : ready) {
                V1ObjectReference targetRef = addr.getTargetRef();
                if (targetRef != null && "Pod".equals(targetRef.getKind())
                        && targetRef.getName() != null) {
                    return targetRef.getName();
                }
            }
        }
        throw noReadyPod(namespace, serviceName);
    }

    private static ApiException noReadyPod(String ns, String svc) {
        // Mirror the shape of a regular ApiException so callers that
        // bubble it up (PortForwardService) log a consistent message.
        return new ApiException(0, "no ready pod backs Service "
                + ns + "/" + svc);
    }
}
