import { TABLE_ID_FLOWS, TABLE_ID_MESSAGES, TABLE_ID_TASKS } from '../utils/contants.js';
import { BigQueryService } from './big-query-service.js';

export class DataImporterService {

    constructor() {
        this.bigQueryService = new BigQueryService();
    }

    async importToBigQuery(payload) {
        let isSuccess = false;

        switch(payload.type) {
            case TABLE_ID_FLOWS:
                isSuccess = await this.bigQueryService.insert(TABLE_ID_FLOWS, this.formatFlowsPayload(payload))
                break;
            case TABLE_ID_TASKS:
                isSuccess = await this.bigQueryService.insert(TABLE_ID_TASKS, this.formatTasksPayload(payload))
                break;
            case TABLE_ID_MESSAGES:
                isSuccess = await this.bigQueryService.insert(TABLE_ID_MESSAGES, this.formatMessagesPayload(payload))
                break;
            default:
                isSuccess = true;
        }

        return isSuccess
    }

    formatFlowsPayload(payload) {
        return {
            result_id: payload.data.result_id,
            result_time: new Date(payload.data.result_time).toISOString().slice(0, -1),
            user_id: payload.data.user_id,
            user_timezone: payload.data.user_timezone,
            user_email: payload.data.user_email,
            user_timezone: payload.data.user_timezone,
            workflow_id: payload.data.workflow_id,
            workflow_name: payload.data.workflow_name,
            response_path: payload.data.response_path,
            request_id: payload.data.request_id,
            outcome: payload.data.outcome,
            platform: payload.data.platform,
            created_at: new Date(payload.data.created_at).toISOString().slice(0, -1),
            bot_id: payload.data.bot_id,
            bot_name: payload.data.bot_name,
            broadcast_id: payload.data.broadcast_id,
            error_name: payload.data.error_name,
            error_type: payload.data.error_type,
            error_message: payload.data.error_message,
        };
    }

    formatTasksPayload(payload) {
        return {
            result_id: payload.data.result_id,
            result_time: new Date(payload.data.result_time).toISOString().slice(0, -1),
            user_id: payload.data.user_id,
            user_timezone: payload.data.user_timezone,
            user_email: payload.data.user_email,
            workflow_id: payload.data.workflow_id,
            workflow_name: payload.data.workflow_name,
            response_path: payload.data.response_path,
            request_id: payload.data.request_id,
            outcome: payload.data.outcome,
            service_name: payload.data.service_name,
            service_id: payload.data.service_id,
            method_name: payload.data.method_name,
            method_id: payload.data.method_id,
            error_name: payload.data.error_name,
            error_type: payload.data.error_type,
            error_message: payload.data.error_message,
            task: JSON.stringify({
              _id: payload.data.task._id,
              name: payload.data.task.name,
              kind: payload.data.task.kind,
              type: payload.data.task.type
            }),
            task_kind: payload.data.task_kind,
            task_type: payload.data.task_type,
            bot_id: payload.data.bot_id
        }
    };

    formatMessagesPayload(payload) {
        return {
            direction: payload.data.direction,
            message_time: new Date(payload.data.message_time).toISOString().slice(0, -1),
            bot_id: payload.data.bot_id,
            bot_name: payload.data.bot_name,
            bot_lang: payload.data.bot_lang,
            message: JSON.stringify({
              type: payload.data.message.type,
              text: payload.data.message.text,
              data: payload.data.message.data
            }),
            message_scope: payload.data.message_scope,
            workflow_id: payload.data.workflow_id,
            task_id: payload.data.task_id,
            was_unhandled: payload.data.was_unhandled,
            was_catch_all: payload.data.was_catch_all,
            was_abandoned: payload.data.was_abandoned,
            is_reply: payload.data.is_reply,
            platform: payload.data.platform,
            response_path: payload.data.response_path
        };
    }
}

