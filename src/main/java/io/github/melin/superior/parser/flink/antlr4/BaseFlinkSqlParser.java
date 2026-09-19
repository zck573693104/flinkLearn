package io.github.melin.superior.parser.flink.antlr4;

import org.antlr.v4.runtime.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for Flink SQL Parser
 */
public abstract class BaseFlinkSqlParser {
    
    protected List<String> errors = new ArrayList<>();
    protected List<String> warnings = new ArrayList<>();
    
    /**
     * Add error message
     */
    protected void addError(String error) {
        errors.add(error);
    }
    
    /**
     * Add warning message
     */
    protected void addWarning(String warning) {
        warnings.add(warning);
    }
    
    /**
     * Get all errors
     */
    public List<String> getErrors() {
        return errors;
    }
    
    /**
     * Get all warnings
     */
    public List<String> getWarnings() {
        return warnings;
    }
    
    /**
     * Check if there are any errors
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }
}
