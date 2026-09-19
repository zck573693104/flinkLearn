package com.bigdata.cep.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PatternCompiler 单元测试
 */
class PatternCompilerTest {
    
    private PatternCompiler compiler;
    
    @BeforeEach
    void setUp() {
        compiler = new PatternCompiler();
    }
    
    @Test
    @DisplayName("编译基本 Pattern")
    void testCompileBasicPattern() throws Exception {
        String patternJson = """
            {
                "name": "test_pattern",
                "times": null,
                "timesMin": null,
                "of": {
                    "first": {
                        "type": "simple",
                        "condition": {
                            "field": "value",
                            "operator": ">",
                            "value": 100
                        }
                    }
                }
            }
            """;
        
        Pattern<CepEvent, ?> pattern = compiler.compile(patternJson);
        
        assertNotNull(pattern);
        assertEquals("test_pattern", pattern.getName());
    }
    
    @Test
    @DisplayName("编译带时间窗口的 Pattern")
    void testCompilePatternWithTimeWindow() throws Exception {
        String patternJson = """
            {
                "name": "time_window_pattern",
                "of": {
                    "first": {
                        "type": "simple",
                        "condition": {
                            "field": "price",
                            "operator": ">",
                            "value": 50
                        }
                    }
                },
                "where": {
                    "type": "strict",
                    "duration": 5,
                    "unit": "minutes"
                }
            }
            """;
        
        Pattern<CepEvent, ?> pattern = compiler.compile(patternJson);
        
        assertNotNull(pattern);
        assertEquals("time_window_pattern", pattern.getName());
    }
    
    @Test
    @DisplayName("编译重复次数 Pattern")
    void testCompileRepeatedPattern() throws Exception {
        String patternJson = """
            {
                "name": "repeated_pattern",
                "times": 3,
                "timesMin": 2,
                "of": {
                    "first": {
                        "type": "simple",
                        "condition": {
                            "field": "count",
                            "operator": ">=",
                            "value": 10
                        }
                    }
                }
            }
            """;
        
        Pattern<CepEvent, ?> pattern = compiler.compile(patternJson);
        
        assertNotNull(pattern);
        assertEquals("repeated_pattern", pattern.getName());
    }
    
    @Test
    @DisplayName("编译多个比较运算符")
    void testCompileMultipleOperators() throws Exception {
        String[] operators = {">", "<", ">=", "<=", "==", "!="};
        
        for (String operator : operators) {
            String patternJson = String.format("""
                {
                    "name": "op_%s",
                    "of": {
                        "first": {
                            "type": "simple",
                            "condition": {
                                "field": "value",
                                "operator": "%s",
                                "value": 100
                            }
                        }
                    }
                }
                """, operator, operator);
            
            Pattern<CepEvent, ?> pattern = compiler.compile(patternJson);
            assertNotNull(pattern);
        }
    }
    
    @Test
    @DisplayName("编译空 Pattern JSON")
    void testCompileEmptyPattern() throws Exception {
        String patternJson = "{}";
        
        Pattern<CepEvent, ?> pattern = compiler.compile(patternJson);
        
        assertNotNull(pattern);
    }
}
