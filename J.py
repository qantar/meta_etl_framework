"""
Notification Service for ETL Framework
Handles email, Slack, webhook, and custom notifications for pipeline events
"""

import os
import json
import smtplib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import requests
import asyncio
import aiohttp
from pathlib import Path
import jinja2


class NotificationLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    SUCCESS = "SUCCESS"


class NotificationType(Enum):
    PIPELINE_START = "pipeline_start"
    PIPELINE_SUCCESS = "pipeline_success"
    PIPELINE_FAILURE = "pipeline_failure"
    TABLE_SUCCESS = "table_success"
    TABLE_FAILURE = "table_failure"
    DATA_QUALITY_ALERT = "data_quality_alert"
    SYSTEM_HEALTH = "system_health"
    MAINTENANCE = "maintenance"
    CUSTOM = "custom"


@dataclass
class NotificationContext:
    """Context information for notifications"""
    notification_type: NotificationType
    level: NotificationLevel
    title: str
    message: str
    pipeline_name: str = ""
    table_name: str = ""
    run_id: str = ""
    timestamp: datetime = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.metadata is None:
            self.metadata = {}


@dataclass
class EmailConfig:
    """Email notification configuration"""
    enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    username: str = ""
    password: str = ""
    use_tls: bool = True
    sender_email: str = ""
    sender_name: str = "ETL Framework"
    recipients: List[str] = None
    
    def __post_init__(self):
        if self.recipients is None:
            self.recipients = []


@dataclass
class SlackConfig:
    """Slack notification configuration"""
    enabled: bool = False
    webhook_url: str = ""
    channel: str = "#etl-alerts"
    username: str = "ETL-Bot"
    icon_emoji: str = ":robot_face:"
    mention_users: List[str] = None
    
    def __post_init__(self):
        if self.mention_users is None:
            self.mention_users = []


@dataclass
class WebhookConfig:
    """Webhook notification configuration"""
    enabled: bool = False
    url: str = ""
    method: str = "POST"
    headers: Dict[str, str] = None
    authentication: Dict[str, str] = None
    timeout: int = 30
    retry_attempts: int = 3
    
    def __post_init__(self):
        if self.headers is None:
            self.headers = {"Content-Type": "application/json"}
        if self.authentication is None:
            self.authentication = {}


class NotificationService:
    """
    Comprehensive notification service for ETL pipeline events
    Supports email, Slack, webhooks, and custom notification channels
    """
    
    def __init__(self, email_config: EmailConfig = None, slack_config: SlackConfig = None,
                 webhook_config: WebhookConfig = None, template_dir: str = None):
        
        self.email_config = email_config or EmailConfig()
        self.slack_config = slack_config or SlackConfig()
        self.webhook_config = webhook_config or WebhookConfig()
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Template engine setup
        self.template_dir = Path(template_dir) if template_dir else Path(__file__).parent / "templates"
        self.template_dir.mkdir(exist_ok=True)
        
        self.jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(str(self.template_dir)),
            autoescape=jinja2.select_autoescape(['html', 'xml'])
        )
        
        # Create default templates if they don't exist
        self._create_default_templates()
        
        # Notification queue for batch processing
        self.notification_queue: List[NotificationContext] = []
        self.batch_size = 10
        self.batch_timeout = 300  # 5 minutes
        
        self.logger.info("Notification service initialized")
    
    def _create_default_templates(self):
        """Create default notification templates"""
        
        # Email HTML template
        email_html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>{{ title }}</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background-color: {% if level == 'ERROR' %}#dc3545{% elif level == 'WARNING' %}#ffc107{% elif level == 'SUCCESS' %}#28a745{% else %}#007bff{% endif %}; color: white; padding: 15px; border-radius: 5px; }
        .content { padding: 20px; border: 1px solid #ddd; border-radius: 5px; margin-top: 10px; }
        .metadata { background-color: #f8f9fa; padding: 10px; border-radius: 3px; margin-top: 10px; }
        .footer { margin-top: 20px; font-size: 12px; color: #666; }
    </style>
</head>
<body>
    <div class="header">
        <h2>{{ title }}</h2>
        <p>{{ level }} - {{ notification_type }}</p>
    </div>
    <div class="content">
        <p><strong>Pipeline:</strong> {{ pipeline_name }}</p>
        {% if table_name %}<p><strong>Table:</strong> {{ table_name }}</p>{% endif %}
        <p><strong>Time:</strong> {{ timestamp.strftime('%Y-%m-%d %H:%M:%S') }}</p>
        {% if run_id %}<p><strong>Run ID:</strong> {{ run_id }}</p>{% endif %}
        
        <h3>Message:</h3>
        <p>{{ message }}</p>
        
        {% if metadata %}
        <div class="metadata">
            <h4>Additional Information:</h4>
            <ul>
                {% for key, value in metadata.items() %}
                <li><strong>{{ key }}:</strong> {{ value }}</li>
                {% endfor %}
            </ul>
        </div>
        {% endif %}
    </div>
    <div class="footer">
        <p>This is an automated message from the ETL Framework.</p>
    </div>
</body>
</html>
"""
        
        # Slack message template
        slack_template = """
{
    "channel": "{{ channel }}",
    "username": "{{ username }}",
    "icon_emoji": "{{ icon_emoji }}",
    "attachments": [
        {
            "color": "{% if level == 'ERROR' %}danger{% elif level == 'WARNING' %}warning{% elif level == 'SUCCESS' %}good{% else %}#007bff{% endif %}",
            "title": "{{ title }}",
            "fields": [
                {
                    "title": "Pipeline",
                    "value": "{{ pipeline_name }}",
                    "short": true
                },
                {% if table_name %}
                {
                    "title": "Table",
                    "value": "{{ table_name }}",
                    "short": true
                },
                {% endif %}
                {
                    "title": "Level",
                    "value": "{{ level }}",
                    "short": true
                },
                {
                    "title": "Time",
                    "value": "{{ timestamp.strftime('%Y-%m-%d %H:%M:%S') }}",
                    "short": true
                }
            ],
            "text": "{{ message }}",
            "footer": "ETL Framework",
            "ts": {{ timestamp.timestamp() | int }}
        }
    ]
}
"""
        
        # Save templates
        templates = {
            "email_html.j2": email_html_template,
            "slack_message.j2": slack_template
        }
        
        for template_name, content in templates.items():
            template_path = self.template_dir / template_name
            if not template_path.exists():
                with open(template_path, 'w') as f:
                    f.write(content)
                self.logger.info(f"Created default template: {template_name}")
    
    async def send_notification(self, context: NotificationContext, 
                               channels: List[str] = None) -> Dict[str, bool]:
        """
        Send notification through specified channels
        Returns dictionary with channel success status
        """
        
        if channels is None:
            channels = self._get_enabled_channels()
        
        results = {}
        
        # Send through each enabled channel
        for channel in channels:
            try:
                if channel == "email" and self.email_config.enabled:
                    success = await self._send_email_notification(context)
                    results["email"] = success
                
                elif channel == "slack" and self.slack_config.enabled:
                    success = await self._send_slack_notification(context)
                    results["slack"] = success
                
                elif channel == "webhook" and self.webhook_config.enabled:
                    success = await self._send_webhook_notification(context)
                    results["webhook"] = success
                
                else:
                    self.logger.warning(f"Channel '{channel}' not enabled or not supported")
                    results[channel] = False
                    
            except Exception as e:
                self.logger.error(f"Failed to send notification via {channel}: {str(e)}")
                results[channel] = False
        
        return results
    
    def _get_enabled_channels(self) -> List[str]:
        """Get list of enabled notification channels"""
        
        channels = []
        if self.email_config.enabled:
            channels.append("email")
        if self.slack_config.enabled:
            channels.append("slack")
        if self.webhook_config.enabled:
            channels.append("webhook")
        
        return channels
    
    async def _send_email_notification(self, context: NotificationContext) -> bool:
        """Send email notification"""
        
        try:
            # Render email template
            template = self.jinja_env.get_template("email_html.j2")
            html_content = template.render(
                **asdict(context),
                channel=self.email_config.sender_email
            )
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"[{context.level.value}] {context.title}"
            msg['From'] = f"{self.email_config.sender_name} <{self.email_config.sender_email}>"
            msg['To'] = ', '.join(self.email_config.recipients)
            
            # Add HTML content
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP(self.email_config.smtp_host, self.email_config.smtp_port) as server:
                if self.email_config.use_tls:
                    server.starttls()
                
                if self.email_config.username and self.email_config.password:
                    server.login(self.email_config.username, self.email_config.password)
                
                server.send_message(msg)
            
            self.logger.info(f"Email notification sent successfully: {context.title}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send email notification: {str(e)}")
            return False
    
    async def _send_slack_notification(self, context: NotificationContext) -> bool:
        """Send Slack notification"""
        
        try:
            # Render Slack message template
            template = self.jinja_env.get_template("slack_message.j2")
            message_json = template.render(
                **asdict(context),
                channel=self.slack_config.channel,
                username=self.slack_config.username,
                icon_emoji=self.slack_config.icon_emoji
            )
            
            # Parse and send message
            message_data = json.loads(message_json)
            
            # Add mentions if configured
            if self.slack_config.mention_users and context.level in [NotificationLevel.ERROR, NotificationLevel.CRITICAL]:
                mentions = ' '.join([f"<@{user}>" for user in self.slack_config.mention_users])
                message_data['text'] = f"{mentions}\n{message_data.get('text', '')}"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(self.slack_config.webhook_url, json=message_data) as response:
                    if response.status == 200:
                        self.logger.info(f"Slack notification sent successfully: {context.title}")
                        return True
                    else:
                        self.logger.error(f"Slack notification failed with status {response.status}")
                        return False
                        
        except Exception as e:
            self.logger.error(f"Failed to send Slack notification: {str(e)}")
            return False
    
    async def _send_webhook_notification(self, context: NotificationContext) -> bool:
        """Send webhook notification"""
        
        try:
            # Prepare webhook payload
            payload = {
                "notification_type": context.notification_type.value,
                "level": context.level.value,
                "title": context.title,
                "message": context.message,
                "pipeline_name": context.pipeline_name,
                "table_name": context.table_name,
                "run_id": context.run_id,
                "timestamp": context.timestamp.isoformat(),
                "metadata": context.metadata
            }
            
            # Setup headers and authentication
            headers = self.webhook_config.headers.copy()
            auth = None
            
            if self.webhook_config.authentication:
                auth_type = self.webhook_config.authentication.get('type', 'bearer')
                
                if auth_type == 'bearer':
                    token = self.webhook_config.authentication.get('token', '')
                    headers['Authorization'] = f"Bearer {token}"
                elif auth_type == 'basic':
                    username = self.webhook_config.authentication.get('username', '')
                    password = self.webhook_config.authentication.get('password', '')
                    auth = aiohttp.BasicAuth(username, password)
            
            # Send webhook
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=self.webhook_config.method,
                    url=self.webhook_config.url,
                    json=payload,
                    headers=headers,
                    auth=auth,
                    timeout=aiohttp.ClientTimeout(total=self.webhook_config.timeout)
                ) as response:
                    if response.status < 400:
                        self.logger.info(f"Webhook notification sent successfully: {context.title}")
                        return True
                    else:
                        self.logger.error(f"Webhook notification failed with status {response.status}")
                        return False
                        
        except Exception as e:
            self.logger.error(f"Failed to send webhook notification: {str(e)}")
            return False
    
    # Convenience methods for common notification types
    def notify_pipeline_start(self, pipeline_name: str, run_id: str, metadata: Dict = None):
        """Notify pipeline start"""
        context = NotificationContext(
            notification_type=NotificationType.PIPELINE_START,
            level=NotificationLevel.INFO,
            title=f"Pipeline Started: {pipeline_name}",
            message=f"ETL pipeline '{pipeline_name}' has started execution.",
            pipeline_name=pipeline_name,
            run_id=run_id,
            metadata=metadata or {}
        )
        
        asyncio.create_task(self.send_notification(context))
    
    def notify_pipeline_success(self, pipeline_name: str, run_id: str, 
                               tables_processed: int, total_rows: int, 
                               duration_seconds: float, metadata: Dict = None):
        """Notify pipeline success"""
        context = NotificationContext(
            notification_type=NotificationType.PIPELINE_SUCCESS,
            level=NotificationLevel.SUCCESS,
            title=f"Pipeline Completed: {pipeline_name}",
            message=f"ETL pipeline '{pipeline_name}' completed successfully. "
                   f"Processed {tables_processed} tables with {total_rows:,} total rows "
                   f"in {duration_seconds:.1f} seconds.",
            pipeline_name=pipeline_name,
            run_id=run_id,
            metadata={
                "tables_processed": tables_processed,
                "total_rows": total_rows,
                "duration_seconds": duration_seconds,
                **(metadata or {})
            }
        )
        
        asyncio.create_task(self.send_notification(context))
    
    def notify_pipeline_failure(self, pipeline_name: str, run_id: str, 
                               error_message: str, failed_table: str = None, 
                               metadata: Dict = None):
        """Notify pipeline failure"""
        context = NotificationContext(
            notification_type=NotificationType.PIPELINE_FAILURE,
            level=NotificationLevel.ERROR,
            title=f"Pipeline Failed: {pipeline_name}",
            message=f"ETL pipeline '{pipeline_name}' failed with error: {error_message}",
            pipeline_name=pipeline_name,
            table_name=failed_table or "",
            run_id=run_id,
            metadata={
                "error_message": error_message,
                "failed_table": failed_table,
                **(metadata or {})
            }
        )
        
        asyncio.create_task(self.send_notification(context))
    
    def notify_data_quality_alert(self, pipeline_name: str, table_name: str, 
                                 quality_score: float, issues: List[str], 
                                 run_id: str = "", metadata: Dict = None):
        """Notify data quality issues"""
        
        level = NotificationLevel.WARNING if quality_score > 80 else NotificationLevel.ERROR
        
        context = NotificationContext(
            notification_type=NotificationType.DATA_QUALITY_ALERT,
            level=level,
            title=f"Data Quality Alert: {table_name}",
            message=f"Data quality issues detected in table '{table_name}'. "
                   f"Quality score: {quality_score:.1f}%. Issues: {', '.join(issues)}",
            pipeline_name=pipeline_name,
            table_name=table_name,
            run_id=run_id,
            metadata={
                "quality_score": quality_score,
                "issues": issues,
                **(metadata or {})
            }
        )
        
        asyncio.create_task(self.send_notification(context))
    
    def notify_system_health(self, status: str, details: Dict, level: NotificationLevel = NotificationLevel.INFO):
        """Notify system health status"""
        context = NotificationContext(
            notification_type=NotificationType.SYSTEM_HEALTH,
            level=level,
            title=f"System Health: {status}",
            message=f"ETL Framework system health check: {status}",
            pipeline_name="System",
            metadata=details
        )
        
        asyncio.create_task(self.send_notification(context))
    
    def create_notification_summary(self, notifications: List[NotificationContext]) -> str:
        """Create a summary of multiple notifications"""
        
        if not notifications:
            return "No notifications to summarize."
        
        summary = f"Notification Summary ({len(notifications)} items):\n\n"
        
        # Group by level
        by_level = {}
        for notif in notifications:
            level = notif.level.value
            if level not in by_level:
                by_level[level] = []
            by_level[level].append(notif)
        
        for level, notifs in by_level.items():
            summary += f"{level}: {len(notifs)} notifications\n"
            for notif in notifs[:3]:  # Show first 3
                summary += f"  - {notif.title} ({notif.pipeline_name})\n"
            if len(notifs) > 3:
                summary += f"  - ... and {len(notifs) - 3} more\n"
            summary += "\n"
        
        return summary
    
    def schedule_batch_notification(self, context: NotificationContext):
        """Add notification to batch queue"""
        self.notification_queue.append(context)
        
        # Send batch if queue is full
        if len(self.notification_queue) >= self.batch_size:
            asyncio.create_task(self._send_batch_notifications())
    
    async def _send_batch_notifications(self):
        """Send queued notifications as a batch"""
        
        if not self.notification_queue:
            return
        
        # Create summary notification
        summary = self.create_notification_summary(self.notification_queue)
        
        batch_context = NotificationContext(
            notification_type=NotificationType.CUSTOM,
            level=NotificationLevel.INFO,
            title=f"ETL Framework Batch Summary ({len(self.notification_queue)} notifications)",
            message=summary,
            pipeline_name="Batch Notification",
            metadata={"notification_count": len(self.notification_queue)}
        )
        
        # Send summary
        await self.send_notification(batch_context)
        
        # Clear queue
        self.notification_queue.clear()
        
        self.logger.info(f"Sent batch notification summary")


# Factory function for easy setup
def create_notification_service(config_dict: Dict = None) -> NotificationService:
    """Create notification service from configuration dictionary"""
    
    if not config_dict:
        return NotificationService()
    
    email_config = None
    if 'email' in config_dict:
        email_config = EmailConfig(**config_dict['email'])
    
    slack_config = None
    if 'slack' in config_dict:
        slack_config = SlackConfig(**config_dict['slack'])
    
    webhook_config = None
    if 'webhook' in config_dict:
        webhook_config = WebhookConfig(**config_dict['webhook'])
    
    template_dir = config_dict.get('template_dir')
    
    return NotificationService(email_config, slack_config, webhook_config, template_dir)


# Usage Example
if __name__ == "__main__":
    import asyncio
    
    # Configuration
    notification_config = {
        'email': {
            'enabled': False,  # Set to True and configure SMTP
            'smtp_host': 'smtp.gmail.com',
            'smtp_port': 587,
            'username': 'your-email@gmail.com',
            'password': 'your-app-password',
            'sender_email': 'your-email@gmail.com',
            'recipients': ['admin@company.com']
        },
        'slack': {
            'enabled': False,  # Set to True and configure webhook
            'webhook_url': 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK',
            'channel': '#etl-alerts',
            'mention_users': ['admin']
        },
        'webhook': {
            'enabled': False,  # Set to True and configure endpoint
            'url': 'https://your-webhook-endpoint.com/notifications',
            'headers': {'Content-Type': 'application/json'},
            'authentication': {
                'type': 'bearer',
                'token': 'your-api-token'
            }
        }
    }
    
    # Create notification service
    notification_service = create_notification_service(notification_config)
    
    async def test_notifications():
        # Test different notification types
        notification_service.notify_pipeline_start("test_pipeline", "run_123")
        
        await asyncio.sleep(1)
        
        notification_service.notify_pipeline_success(
            "test_pipeline", "run_123", 
            tables_processed=5, total_rows=10000, duration_seconds=120.5
        )
        
        notification_service.notify_data_quality_alert(
            "test_pipeline", "customers", 
            quality_score=75.5, issues=["Missing values", "Duplicates"], 
            run_id="run_123"
        )
    
    # Run test
    asyncio.run(test_notifications())
    print("Notification tests completed")