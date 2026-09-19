import axios from 'axios'

const API_BASE_URL = '/api/cep'

/**
 * 获取所有 CEP 规则
 */
export function getAllRules() {
  return axios.get(`${API_BASE_URL}/rules`)
}

/**
 * 创建新规则
 */
export function createRule(config) {
  return axios.post(`${API_BASE_URL}/rules`, config)
}

/**
 * 更新现有规则
 */
export function updateRule(ruleId, config) {
  return axios.put(`${API_BASE_URL}/rules/${ruleId}`, config)
}

/**
 * 删除规则
 */
export function deleteRule(ruleId) {
  return axios.delete(`${API_BASE_URL}/rules/${ruleId}`)
}

/**
 * 获取规则详情
 */
export function getRule(ruleId) {
  return axios.get(`${API_BASE_URL}/rules/${ruleId}`)
}
