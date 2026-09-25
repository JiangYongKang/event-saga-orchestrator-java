package com.github.highcumontoa.eventsagaorchestratorjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.eventsagaorchestratorjava.flow.ControllableStepAction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * REST 端到端集成测试：覆盖创建、幂等重复、乱序冲突、补偿失败终态与错误响应归一化。
 * 每个类用例后重建上下文，保证内存存储与故障注入互不干扰。
 */
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class SagaApiIntegrationTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Autowired
    private WebApplicationContext wac;

    @Autowired
    private ControllableStepAction createOrderAction;

    @Autowired
    private ControllableStepAction debitPaymentAction;

    private MockMvc mvc;

    private MockMvc mock() {
        if (mvc == null) {
            mvc = MockMvcBuilders.webAppContextSetup(wac).build();
        }
        return mvc;
    }

    @Test
    @DisplayName("HTTP 端到端：创建 -> 4步推进 -> COMPLETED")
    void endToEndHappyPath() throws Exception {
        mock().perform(post("/api/sagas")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSON.writeValueAsString(Map.of("businessKey", "k1", "flowType", "ORDER"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.sagaState").value("RUNNING"));

        for (int i = 0; i < 4; i++) {
            mock().perform(post("/api/sagas/k1/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(JSON.writeValueAsString(Map.of(
                                    "eventId", "evt-step-" + i,
                                    "stepIndex", i))))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("ACCEPTED"));
        }

        mock().perform(get("/api/sagas/k1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("COMPLETED"))
                .andExpect(jsonPath("$.steps.length()").value(4));

        // 同一 eventId 重复投递 => 409 + 结构化重复原因
        MvcResult dup = mock().perform(post("/api/sagas/k1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSON.writeValueAsString(Map.of(
                                "eventId", "evt-step-0",
                                "stepIndex", 0))))
                .andExpect(status().isConflict())
                .andReturn();
        JsonNode body = JSON.readTree(dup.getResponse().getContentAsString());
        assertEquals("DUPLICATE", body.get("status").asText());
        assertTrue(body.get("reasonCode").asText().contains("DUPLICATE_EVENT"));
    }

    @Test
    @DisplayName("HTTP：乱序事件返回 409，原因码可区分")
    void outOfOrderOverHttp() throws Exception {
        mock().perform(post("/api/sagas")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JSON.writeValueAsString(Map.of("businessKey", "k2", "flowType", "ORDER"))))
                .andExpect(status().isOk());

        MvcResult result = mock().perform(post("/api/sagas/k2/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSON.writeValueAsString(Map.of(
                                "eventId", "evt-ahead",
                                "stepIndex", 3))))
                .andExpect(status().isConflict())
                .andReturn();
        JsonNode body = JSON.readTree(result.getResponse().getContentAsString());
        assertEquals("STALE", body.get("status").asText());
        assertTrue(body.get("reasonCode").asText().contains("OUT_OF_ORDER"));
    }

    @Test
    @DisplayName("HTTP：未知流程返回 400 且不泄露内部细节")
    void unknownFlowReturnsSafeError() throws Exception {
        MvcResult result = mock().perform(post("/api/sagas")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSON.writeValueAsString(Map.of(
                                "businessKey", "k3", "flowType", "UNKNOWN_X"))))
                .andExpect(status().isBadRequest())
                .andReturn();
        JsonNode body = JSON.readTree(result.getResponse().getContentAsString());
        assertEquals("VALIDATION_ERROR", body.get("errorCode").asText());
    }

    @Test
    @DisplayName("HTTP：补偿重试耗尽 -> DEAD_LETTER 结果与 COMPENSATION_FAILED 终态可查询")
    void compensationFailureOverHttp() throws Exception {
        createOrderAction.failForward(100);
        debitPaymentAction.failCompensation(100);

        mock().perform(post("/api/sagas")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JSON.writeValueAsString(Map.of("businessKey", "k4", "flowType", "ORDER"))))
                .andExpect(status().isOk());

        for (int i = 0; i < 2; i++) {
            mock().perform(post("/api/sagas/k4/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(JSON.writeValueAsString(Map.of(
                                    "eventId", "evt-k4-" + i, "stepIndex", i))))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("ACCEPTED"));
        }

        mock().perform(post("/api/sagas/k4/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSON.writeValueAsString(Map.of(
                                "eventId", "evt-k4-2", "stepIndex", 2))))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.status").value("DEAD_LETTER"))
                .andExpect(jsonPath("$.sagaState").value("COMPENSATION_FAILED"));

        MvcResult state = mock().perform(get("/api/sagas/k4"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("COMPENSATION_FAILED"))
                .andReturn();
        JsonNode body = JSON.readTree(state.getResponse().getContentAsString());
        JsonNode debitStep = body.get("steps").get(1);
        assertEquals(3, debitStep.get("compensationAttempts").asInt(),
                "补偿必须按上限重试 3 次");
        assertTrue("FAILED".equals(debitStep.get("status").asText()));
    }
}
