package com.crypto.flink.controller;

import com.crypto.flink.rules.DetectionRule;
import com.crypto.flink.rules.DetectionRuleService;
import com.crypto.flink.rules.DetectionRules;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/rules")
@Tag(name = "Detection Rules", description = "감지 임계치. 바꾸면 Flink Job 이 2~3초 안에 새 값으로 다시 뜬다")
public class RulesController {

    private final DetectionRuleService service;

    public RulesController(DetectionRuleService service) {
        this.service = service;
    }

    @GetMapping
    @Operation(summary = "기본 규칙과 종목별 덮어쓰기", description = "effective 는 덮어쓰기를 기본값에 합친 종목별 유효 규칙")
    public Map<String, Object> rules() {
        return view(service.current());
    }

    @PutMapping("/{code}")
    @Operation(summary = "규칙 변경", description = "code 가 default 또는 * 면 기본 규칙 전체 교체(모든 값 필수). 종목 코드면 준 값만 덮어쓴다")
    public Map<String, Object> update(@PathVariable String code, @RequestBody DetectionRule change) {
        try {
            return view(service.update(normalize(code), change));
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
    }

    @DeleteMapping("/{code}")
    @Operation(summary = "종목 덮어쓰기 삭제", description = "그 종목은 다시 기본 규칙을 따른다")
    public Map<String, Object> delete(@PathVariable String code) {
        try {
            return view(service.deleteOverride(normalize(code)));
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
    }

    private static String normalize(String code) {
        String upper = code.toUpperCase();
        return upper.equals("DEFAULT") ? DetectionRule.DEFAULT_CODE : upper;
    }

    private static Map<String, Object> view(DetectionRules rules) {
        List<DetectionRule> effective = rules.overrides().stream()
                .map(override -> rules.resolve(override.getCode()))
                .toList();
        return Map.of(
                "defaults", rules.defaults(),
                "overrides", rules.overrides(),
                "effective", effective);
    }
}
