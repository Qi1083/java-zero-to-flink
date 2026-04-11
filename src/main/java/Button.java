interface ClickListener {
    void onClick();
}

public class Button {
    private ClickListener listener;

    public void setListener(ClickListener listener) {
        this.listener = listener;
    }

    public void click() {
        if (listener != null) listener.onClick();
    }

    public static void main(String[] args) {
        Button btn = new Button();
        // 匿名内部类
        btn.setListener(new ClickListener() {
            @Override
            public void onClick() {
                System.out.println("click");
            }
        });
        btn.click();
    }

}

