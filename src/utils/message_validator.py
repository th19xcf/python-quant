#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
消息格式验证模块
用于验证插件间通信的消息格式
"""

import time
import uuid
from typing import Dict, Any

import jsonschema

# 定义消息格式Schema
MESSAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "version": {
            "type": "string",
            "pattern": r"^\d+\.\d+$",
            "description": "消息版本"
        },
        "timestamp": {
            "type": "number",
            "minimum": 0,
            "description": "时间戳"
        },
        "sender": {
            "type": "string",
            "minLength": 1,
            "description": "发送者插件名"
        },
        "recipient": {
            "type": "string",
            "minLength": 1,
            "description": "接收者插件名，'*'表示广播"
        },
        "message_type": {
            "type": "string",
            "minLength": 1,
            "description": "消息类型"
        },
        "data": {
            "type": ["object", "array", "string", "number", "boolean", "null"],
            "description": "消息内容"
        },
        "priority": {
            "type": "integer",
            "minimum": 0,
            "maximum": 9,
            "description": "优先级，0-9，0最高"
        },
        "correlation_id": {
            "type": "string",
            "minLength": 1,
            "description": "关联ID，用于请求-响应配对"
        },
        "metadata": {
            "type": "object",
            "description": "附加元数据"
        }
    },
    "required": [
        "version",
        "timestamp",
        "sender",
        "recipient",
        "message_type",
        "data",
        "priority",
        "correlation_id",
        "metadata"
    ]
}

# 请求消息扩展Schema
REQUEST_SCHEMA = {
    "type": "object",
    "properties": {
        **MESSAGE_SCHEMA["properties"],
        "data": {
            "type": "object",
            "properties": {
                "method": {
                    "type": "string",
                    "minLength": 1,
                    "description": "请求方法名"
                },
                "params": {
                    "type": "object",
                    "description": "请求参数"
                },
                "timeout": {
                    "type": "number",
                    "minimum": 1,
                    "maximum": 60,
                    "description": "超时时间（秒）"
                }
            },
            "required": ["method", "params"]
        }
    },
    "required": MESSAGE_SCHEMA["required"]
}

# 响应消息扩展Schema
RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        **MESSAGE_SCHEMA["properties"],
        "data": {
            "type": "object",
            "properties": {
                "result": {
                    "type": ["object", "array", "string", "number", "boolean", "null"],
                    "description": "响应结果"
                },
                "error": {
                    "type": ["string", "null"],
                    "description": "错误信息"
                }
            }
        }
    },
    "required": MESSAGE_SCHEMA["required"]
}

# 事件消息扩展Schema
EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        **MESSAGE_SCHEMA["properties"],
        "event_name": {
            "type": "string",
            "minLength": 1,
            "description": "事件名称"
        }
    },
    "required": MESSAGE_SCHEMA["required"]
}


class MessageValidator:
    """
    消息格式验证器
    """
    
    @staticmethod
    def validate_message(message: Dict[str, Any]) -> bool:
        """
        验证消息格式
        
        Args:
            message: 消息内容
            
        Returns:
            bool: 验证是否通过
            
        Raises:
            jsonschema.ValidationError: 验证失败时抛出
        """
        jsonschema.validate(instance=message, schema=MESSAGE_SCHEMA)
        return True
    
    @staticmethod
    def validate_request(message: Dict[str, Any]) -> bool:
        """
        验证请求消息格式
        
        Args:
            message: 请求消息内容
            
        Returns:
            bool: 验证是否通过
            
        Raises:
            jsonschema.ValidationError: 验证失败时抛出
        """
        jsonschema.validate(instance=message, schema=REQUEST_SCHEMA)
        return True
    
    @staticmethod
    def validate_response(message: Dict[str, Any]) -> bool:
        """
        验证响应消息格式
        
        Args:
            message: 响应消息内容
            
        Returns:
            bool: 验证是否通过
            
        Raises:
            jsonschema.ValidationError: 验证失败时抛出
        """
        jsonschema.validate(instance=message, schema=RESPONSE_SCHEMA)
        return True
    
    @staticmethod
    def validate_event(message: Dict[str, Any]) -> bool:
        """
        验证事件消息格式
        
        Args:
            message: 事件消息内容
            
        Returns:
            bool: 验证是否通过
            
        Raises:
            jsonschema.ValidationError: 验证失败时抛出
        """
        jsonschema.validate(instance=message, schema=EVENT_SCHEMA)
        return True
    
    @staticmethod
    def validate(message: Dict[str, Any], message_type: str = None) -> bool:
        """
        通用验证方法，根据消息类型自动选择验证Schema
        
        Args:
            message: 消息内容
            message_type: 消息类型，可选
            
        Returns:
            bool: 验证是否通过
            
        Raises:
            jsonschema.ValidationError: 验证失败时抛出
        """
        if message_type == "request" or ("data" in message and isinstance(message["data"], dict) and "method" in message["data"]):
            return MessageValidator.validate_request(message)
        elif message_type == "response" or ("data" in message and isinstance(message["data"], dict) and ("result" in message["data"] or "error" in message["data"])):
            return MessageValidator.validate_response(message)
        elif message_type == "event" or "event_name" in message:
            return MessageValidator.validate_event(message)
        else:
            return MessageValidator.validate_message(message)
    
    @staticmethod
    def generate_message_id() -> str:
        """
        生成唯一消息ID
        
        Returns:
            str: 消息ID
        """
        return str(uuid.uuid4())
    
    @staticmethod
    def create_response(message: Dict[str, Any], result: Any = None, error: str = None) -> Dict[str, Any]:
        """
        创建响应消息
        
        Args:
            message: 原始请求消息
            result: 响应结果
            error: 错误信息
            
        Returns:
            Dict[str, Any]: 响应消息
        """
        return {
            "version": message.get("version", "1.0"),
            "timestamp": time.time(),
            "sender": message["recipient"],
            "recipient": message["sender"],
            "message_type": "response",
            "data": {
                "result": result,
                "error": error
            },
            "priority": message.get("priority", 0),
            "correlation_id": message["correlation_id"],
            "metadata": {
                "plugin_version": message.get("metadata", {}).get("plugin_version", "unknown")
            }
        }
    
    @staticmethod
    def normalize_message(message: Dict[str, Any]) -> Dict[str, Any]:
        """
        标准化消息格式
        
        Args:
            message: 原始消息
            
        Returns:
            Dict[str, Any]: 标准化后的消息
        """
        normalized = {
            "version": message.get("version", "1.0"),
            "timestamp": message.get("timestamp", time.time()),
            "sender": message.get("sender", "unknown"),
            "recipient": message.get("recipient", "*"),
            "message_type": message.get("message_type", "data"),
            "data": message.get("data", {}),
            "priority": message.get("priority", 0),
            "correlation_id": message.get("correlation_id", MessageValidator.generate_message_id()),
            "metadata": message.get("metadata", {})
        }
        
        return normalized
    
    @staticmethod
    def is_valid(message: Dict[str, Any]) -> bool:
        """
        检查消息是否有效，不抛出异常
        
        Args:
            message: 消息内容
            
        Returns:
            bool: 消息是否有效
        """
        try:
            return MessageValidator.validate(message)
        except jsonschema.ValidationError:
            return False
        except Exception:
            return False
    
    @staticmethod
    def get_validation_error(message: Dict[str, Any]) -> str:
        """
        获取验证错误信息
        
        Args:
            message: 消息内容
            
        Returns:
            str: 错误信息
        """
        try:
            MessageValidator.validate(message)
            return ""
        except jsonschema.ValidationError as e:
            return str(e)
        except Exception as e:
            return f"验证失败: {str(e)}"