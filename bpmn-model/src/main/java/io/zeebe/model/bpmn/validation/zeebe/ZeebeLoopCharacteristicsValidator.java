/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.model.bpmn.validation.zeebe;

import io.zeebe.model.bpmn.impl.ZeebeConstants;
import io.zeebe.model.bpmn.instance.zeebe.ZeebeLoopCharacteristics;
import org.camunda.bpm.model.xml.validation.ModelElementValidator;
import org.camunda.bpm.model.xml.validation.ValidationResultCollector;

public class ZeebeLoopCharacteristicsValidator implements ModelElementValidator<ZeebeLoopCharacteristics> {

  @Override
  public Class<ZeebeLoopCharacteristics> getElementType() {
    return ZeebeLoopCharacteristics.class;
  }

  @Override
  public void validate(
    ZeebeLoopCharacteristics element, ValidationResultCollector validationResultCollector) {

    final String inputCollection = element.getInputCollection();

    if (inputCollection == null || inputCollection.isEmpty()) {
      validationResultCollector.addError(0, String.format(
        "Attribute '%s' must be present and not empty",
        ZeebeConstants.ATTRIBUTE_INPUT_COLLECTION));
    }
  }
}
