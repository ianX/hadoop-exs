package org.rs.client.ui.component;

import javax.swing.JPanel;

public class ContentPanel<T extends ItemPanel> extends JPanel {

	private MainPane mainPane;
	private ToolbarPanel toolbarPanel;
	private ListPanel<T> listPanel;
	private ContentScrollPane contentScrollPane;

	public ContentPanel(MainPane mainPane) {
		// TODO Auto-generated constructor stub
		this.mainPane = mainPane;
		toolbarPanel = new ToolbarPanel(mainPane);
		listPanel = new ListPanel<T>(mainPane);
		contentScrollPane = new ContentScrollPane(mainPane, listPanel);
	}

	public void add(T t) {
		this.listPanel.add(t);
	}
	
	public void remove(T t) {
		this.listPanel.remove(t);
	}
}
