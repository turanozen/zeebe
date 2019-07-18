package io.zeebe.model.bpmn.instance.zeebe;

import io.zeebe.model.bpmn.instance.BpmnModelElementInstance;

public interface ZeebeLoopCharacteristics extends BpmnModelElementInstance {

  String getInputCollection();

  void setInputCollection(String inputCollection);

  String getInputElement();

  void setInputElement(String inputElement);

}


