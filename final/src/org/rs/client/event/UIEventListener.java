package org.rs.client.event;

import java.util.EventListener;

public interface UIEventListener extends EventListener {

	public void handleUIEvent(UIEvent e);
}
