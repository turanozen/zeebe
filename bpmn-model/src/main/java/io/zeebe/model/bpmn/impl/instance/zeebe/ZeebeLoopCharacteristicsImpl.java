package io.zeebe.model.bpmn.impl.instance.zeebe;

import io.zeebe.model.bpmn.impl.BpmnModelConstants;
import io.zeebe.model.bpmn.impl.ZeebeConstants;
import io.zeebe.model.bpmn.impl.instance.BpmnModelElementInstanceImpl;
import io.zeebe.model.bpmn.instance.zeebe.ZeebeLoopCharacteristics;
import org.camunda.bpm.model.xml.ModelBuilder;
import org.camunda.bpm.model.xml.impl.instance.ModelTypeInstanceContext;
import org.camunda.bpm.model.xml.type.ModelElementTypeBuilder;
import org.camunda.bpm.model.xml.type.attribute.Attribute;

public class ZeebeLoopCharacteristicsImpl extends BpmnModelElementInstanceImpl implements ZeebeLoopCharacteristics {

  private static Attribute<String> inputCollectionAttribute;
  private static Attribute<String> inputElementAttribute;

  public ZeebeLoopCharacteristicsImpl(ModelTypeInstanceContext instanceContext) {
    super(instanceContext);
  }

  public static void registerType(ModelBuilder modelBuilder) {
    final ModelElementTypeBuilder typeBuilder =
      modelBuilder
        .defineType(ZeebeLoopCharacteristics.class, ZeebeConstants.ELEMENT_LOOP_CHARACTERISTICS)
        .namespaceUri(BpmnModelConstants.ZEEBE_NS)
        .instanceProvider(ZeebeLoopCharacteristicsImpl::new);

    inputCollectionAttribute =
      typeBuilder
        .stringAttribute(ZeebeConstants.ATTRIBUTE_INPUT_COLLECTION)
        .namespace(BpmnModelConstants.ZEEBE_NS)
        .required()
        .build();

    inputElementAttribute =
      typeBuilder
        .stringAttribute(ZeebeConstants.ATTRIBUTE_INPUT_ELEMENT)
        .namespace(BpmnModelConstants.ZEEBE_NS)
        .build();

    typeBuilder.build();
  }

  @Override
  public String getInputCollection() {
    return inputCollectionAttribute.getValue(this);
  }

  @Override
  public void setInputCollection(String inputCollection) {
    inputCollectionAttribute.setValue(this, inputCollection);
  }

  @Override
  public String getInputElement() {
    return inputElementAttribute.getValue(this);
  }

  @Override
  public void setInputElement(String inputElement) {
    inputElementAttribute.setValue(this, inputElement);
  }
}
