package org.rs.client.event;

import java.util.EventObject;

import org.rs.client.ui.UI;

public class UIEvent extends EventObject {

	private static final long serialVersionUID = -8345883679988260575L;

	private EventType eventType;

	private UI ui;

	private boolean useParam = false;

	public UIEvent(UI source, boolean useParam) {
		super(source);
		// TODO Auto-generated constructor stub
		ui = source;
		eventType = ui.getEventType();
		this.useParam = useParam;
	}

	public UIEvent useParam() {
		useParam = true;
		return this;
	}

	public UI getSource() {
		return ui;
	}

	public EventType getEventType() {
		return this.eventType;
	}

	public String[] getParams() {
		if (useParam)
			return ui.getParams();
		else
			return null;
	}
}
