package com.kubrik.mex.ui.migration;

import javafx.beans.binding.BooleanBinding;
import javafx.scene.layout.Region;

/** Contract every wizard step implements. */
public interface WizardStep {
    String title();
    Region view();
    /** Binding that lights up when the step is fully satisfied; drives the Next button. */
    BooleanBinding validProperty();
    /** Called when the step becomes current. Lets steps refresh data from upstream changes. */
    default void onEnter() {}
}
