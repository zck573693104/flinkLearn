# -*- coding: utf-8 -*-
import os
import re

def replace_chinese_in_java_files(root_dir):
    """Replace Chinese characters in Java files with English equivalents"""
    
    replacements = {
        '基于 ANTLR4': 'Based on ANTLR4',
        '基于 Visitor 模式': 'Based on Visitor Pattern',
        '完全独立的实现': 'Independent Implementation',
        '不依赖': 'Does not depend on',
        '支持': 'Supports',
        '提取': 'Extract',
        '创建': 'Create',
        '解析': 'Parse',
        '访问': 'Visit',
        '构建': 'Build',
        '检查': 'Check',
        '处理': 'Process',
        '移除': 'Remove',
        '添加': 'Add',
        '失败': 'Failed',
        '目标表': 'Target Table',
        '源表': 'Source Table',
        '临时表': 'Temporary Table',
        '窗口函数': 'Window Function',
        '主查询': 'Main Query',
        'CTE 定义': 'CTE Definition',
        '视为': 'Treat as',
        '函数名': 'Function Name',
        '表路径': 'Table Path',
        '子句': 'Clause',
        '错误监听器': 'Error Listener',
        '自定义': 'Custom',
        '处理器': 'Handler',
        '表级血缘提取器': 'Table Lineage Extractor',
        '从 SQL 语句提取表级血缘': 'Extract table lineage from SQL statement',
        '从 SQL 语句提取表级血': 'Extract table lineage from SQL statement',
        '创建词法分析器': 'Create lexer',
        '创建解析器': 'Create parser',
        '移除默认错误监听器': 'Remove default error listener',
        '避免输出到控制台': 'Avoid output to console',
        '添加自定义错误处理器': 'Add custom error handler',
        'SQL 解析失败': 'SQL parse failed',
        '创建血缘提取访问者': 'Create lineage visitor',
        '构建血缘关系': 'Build lineage relationship',
        '血缘提取失败': 'Lineage extraction failed',
        'ANTLR4 Visitor 实现': 'ANTLR4 Visitor Implementation',
        '遍历 AST 并提取血缘信息': 'Traverse AST and extract lineage info',
        '提取目标表': 'Extract target table',
        '插入模式继续访问': 'Insert mode continue visit',
        '访问所有 CTE 定义': 'Visit all CTE definitions',
        '访问主查询': 'Visit main query',
        '将 CTE 视为临时源表': 'Treat CTE as temporary source table',
        '访问内部查询': 'Visit internal query',
        '访问子句': 'Visit clause',
        '提取 CTE 名称': 'Extract CTE name',
        '检查是否有窗口函数': 'Check if has window function',
        '检查 WINDOW 子句': 'Check WINDOW clause',
        '这里可以添加更复杂的逻辑': 'Here can add more complex logic',
        '判断是否为窗口函数': 'Judge if is window function',
        '提取函数名': 'Extract function name',
        '检查是否': 'Check if',
        '有窗口函数': 'has window function',
        '处理表路径': 'Process table path',
        '递归处理': 'Recursive process',
        '检查是否为': 'Check if is',
        '表引用': 'Table reference',
        '返回 null': 'Return null',
        '添加到源表列表': 'Add to source table list',
        '没有表引用': 'No table reference',
        '获取子节点': 'Get child node',
        '遍历子节点': 'Traverse child nodes',
        '跳过': 'Skip',
        '调用父类方法': 'Call parent class method',
        '标记': 'Mark',
        '设置': 'Set',
        '获取': 'Get',
        '设置标志': 'Set flag',
        '获取标志': 'Get flag',
    }
    
    java_files = []
    for root, dirs, files in os.walk(root_dir):
        # Skip target directory
        if 'target' in root:
            continue
        for file in files:
            if file.endswith('.java'):
                java_files.append(os.path.join(root, file))
    
    print(f"Found {len(java_files)} Java files")
    
    for file_path in java_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            for chinese, english in replacements.items():
                content = content.replace(chinese, english)
            
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"Fixed: {file_path}")
                
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print("Done!")

if __name__ == '__main__':
    replace_chinese_in_java_files(r'D:\project\flinkLearn\src\main\java')
