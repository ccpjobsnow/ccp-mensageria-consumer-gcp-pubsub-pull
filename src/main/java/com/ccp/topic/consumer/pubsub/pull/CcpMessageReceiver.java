package com.ccp.topic.consumer.pubsub.pull;

import java.util.function.Function;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.especifications.db.utils.CcpEntity;
import com.ccp.exceptions.process.CcpAsyncProcess;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

public class CcpMessageReceiver implements MessageReceiver {
	private final Function<CcpJsonRepresentation, CcpJsonRepresentation> jnAsyncBusinessNotifyError;
	
	private final Function<CcpJsonRepresentation, CcpJsonRepresentation> notifyError ;

	private final CcpEntity asyncTask;
	
	public final String name;

	
	public CcpMessageReceiver(Function<CcpJsonRepresentation, CcpJsonRepresentation> notifyError,
			 CcpEntity asyncTask,
			String name,  Function<CcpJsonRepresentation, CcpJsonRepresentation> jnAsyncBusinessNotifyError) {
		this.notifyError = notifyError;
		this.asyncTask = asyncTask;
		this.jnAsyncBusinessNotifyError = jnAsyncBusinessNotifyError;
		this.name = name;
	}

	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		try {
			ByteString data = message.getData();
			String receivedMessage = data.toStringUtf8();
			CcpJsonRepresentation mdMessage = new CcpJsonRepresentation(receivedMessage);
			try {
				Function<CcpJsonRepresentation, CcpJsonRepresentation> task = msg -> CcpAsyncProcess.executeProcess(this.name, msg, this.asyncTask, this.jnAsyncBusinessNotifyError);
				task.apply(mdMessage);
			} catch (Throwable e) {
				CcpJsonRepresentation put = CcpConstants.EMPTY_JSON.put("topic", this.name).put("values", mdMessage);
				throw new RuntimeException(put.asPrettyJson(), e);
			}
			consumer.ack();
		} catch (Throwable e) {
			CcpJsonRepresentation values = new CcpJsonRepresentation(e);
			CcpJsonRepresentation execute = this.notifyError.apply(values.renameKey("message", "msg"));
			this.notifyError.apply(execute);
			consumer.nack();
		}
		
	}

}
