package com.ning.protocol;

import com.google.common.base.Objects;
import com.ning.buffer.ManagedBuffer;

/**
 * Abstract class for messages which optionally
 * contain a body kept in a separate buffer.
 * 消息抽象类  对于body是否包含在单独的buffer中是可选
 *
 */
public abstract class AbstractMessage implements Message{
    private final ManagedBuffer body;
    private final boolean isBodyInFrame;

    protected AbstractMessage() {
        this(null, false);
    }
    protected AbstractMessage(ManagedBuffer body, boolean isBodyInFrame) {
        this.body = body;
        this.isBodyInFrame = isBodyInFrame;
    }
    @Override
    public ManagedBuffer body() {
        return body;
    }

    @Override
    public boolean isBodyInFrame() {
        return isBodyInFrame;
    }

    protected boolean equals(AbstractMessage other) {
        return isBodyInFrame == other.isBodyInFrame &&
                Objects.equal(body, other.body);
    }
}
