package com.foresee.hackathon.odata.test;

import com.google.common.collect.Lists;
import com.hevelian.olastic.core.elastic.utils.AggregationUtils;
import org.elasticsearch.ResourceNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.stream.Collectors;

/**
 * The core service that fetch one or a list of respondents
 */
@Service
class RespondentGen {


    @Autowired
    private RespondentDataRepository respondentDataRepository;


    private List<RespondentDetail> convert(@NonNull final List<String> respondentIds, final Project project) {
        if (respondentIds.isEmpty()) {
            return Collections.emptyList();
        }

        List<List<String>> partitions = Lists.partition(respondentIds, 1000);
        ArrayList<RespondentData> list = partitions.stream()
                .map(partitionedIds -> respondentDataRepository.findByIdsIn(partitionedIds))
                .reduce(Lists.newArrayList(), (o, respondentData) -> {
                    o.addAll(respondentData);
                    return o;
                }, (objects, objects2) -> {
                    objects.addAll(objects2);
                    return objects;
                });

        // build the Map of <respondetId, RespondentDetail> from raw RespondentData list
        Map<String, List<RespondentData>> listMap = list.stream().collect(Collectors.groupingBy(
                RespondentData::getRespondentId, Collectors.toList()));
        OptionalInt optionalInt = list.stream().map(RespondentData::getVersion).filter(Objects::nonNull)
                .mapToInt(Integer::intValue).max();
        int maxVersion = optionalInt.isPresent() ? optionalInt.getAsInt()
                : questionService.getMaxVersion(project.getModelInstanceId());
        Map<String, RespondentDetail> respondentDetailMap = listMap.values().stream()
                .filter(respondentDataList -> !respondentDataList.isEmpty())
                .map(respondentDataList -> toRespondentDetail(respondentDataList, project.getFeedbackRatingQuestionId(),
                        project.getFeedbackTopicQuestionId(), maxVersion))
                .collect(Collectors.toMap(RespondentDetail::getRespondentId, Function.identity()));
        // use the original list to make a new list with correct order.
        return respondentIds.stream().map(respondentDetailMap::get).collect(Collectors.toList());
    }

    /**
     * a utility function that convert a list of RespondentData to a single RespondentDetail
     * It calls questionService and answerService to fetch extra information which is not in the list.
     *
     * @param list
     * @param ratingQuestionId
     * @param topicQuestionId
     * @param maxVersion
     * @return
     */
    private RespondentDetail toRespondentDetail(List<RespondentData> list, String ratingQuestionId,
                                                String topicQuestionId, int maxVersion) {
        RespondentDetail respondent = new RespondentDetail();
        RespondentData first = list.get(0);
        respondent.setRespondentId(first.getRespondentId());
        //See if this is a staged respondent
        boolean overrideToMaxVersion = first.getQuestionId().equalsIgnoreCase("env") &&
                first.getFreeFormRespText().equalsIgnoreCase("stg");
        // setup basic information using the first element
        respondent.setModelInstanceId(first.getModelInstanceId());
        respondent.setResponseDate(first.getResponseDate());
        respondent.setExperienceDate(first.getExperienceDate());
        respondent.setUrl(first.getUrl());
        respondent.setGlobalId(first.getGlobalId());
        respondent.setSessionId(first.getSessionId());
        respondent.setDeleted(first.getDeleted().booleanValue());
        Integer version = (first.getVersion() == null || overrideToMaxVersion) ? maxVersion : first.getVersion();
        List<QuestionAnswer> questionAnswers = list.stream().map(respondentData -> {
            Question question = null;
            try {
                if (AggregationUtils.looksLikeAStandardQuestion(respondentData.getQuestionId())) {
                    question = questionService.getQuestion(respondentData.getQuestionId(), version);
                }
            } catch (ResourceNotFoundException ex) {
                log.info("question not found {}, version {}", respondentData.getQuestionId(), version);
            }
            question = (question == null) ? createQuestion(respondentData, version) : question;
            QuestionAnswer questionAnswer = new QuestionAnswer();
            questionAnswer.setQuestionId(question.getQuestionId());
            questionAnswer.setParentQuestionId(question.getParentQuestionId());
            questionAnswer.setParentAnswerId(question.getParentAnswerId());
            questionAnswer.setQuestionLabel(question.getQuestionLabel());
            questionAnswer.setQuestionText(question.getQuestionText());
            questionAnswer.setQuestionKind(question.getQuestionKind());
            questionAnswer.setQuestionType(question.getQuestionType());
            questionAnswer.setDisplayType(question.getDisplayType());
            // get answer information
            if (!StringUtils.isEmpty(respondentData.getAnswerId())) {
                Answer answer = answerService.getAnswer(respondentData.getAnswerId(), version);
                if (answer != null) {
                    questionAnswer.setAnswerId(answer.getAnswerId());
                    questionAnswer.setAnswerValue(answer.getAnswerValue());
                    questionAnswer.setAnswerLabel(answer.getAnswerLabel());
                    if (question.getQuestionId().equals(ratingQuestionId)) {
                        respondent.setRating(answer.getAnswerValue().intValue());
                    } else if (question.getQuestionId().equals(topicQuestionId)) {
                        respondent.setTopicName(answer.getAnswerLabel());
                    }
                }
            }
            if (respondentData.getFreeFormRespText() != null
                    && RespondentDataMaskUtils.isIpAddress(respondentData.getQuestionId(), respondentData.getFreeFormRespText())) {
                questionAnswer.setFreeFormRespText(RespondentDataMaskUtils.getIpAddressMask());
            } else {
                questionAnswer.setFreeFormRespText(respondentData.getFreeFormRespText());
            }
            if (question.getDisplayOrder() != null) {
                questionAnswer.setDisplayOrder(question.getDisplayOrder());
            } else {
                questionAnswer.setDisplayOrder(9999);
            }
            return questionAnswer;
        }).collect(Collectors.toList());
        respondent.setAnswers(questionAnswers);
        return respondent;
    }

    private Question createQuestion(RespondentData respondentData, Integer version) {
        Question question = new Question();
        question.setQuestionId(respondentData.getQuestionId());
        question.setVersion(version);
        question.setQuestionLabel(respondentData.getQuestionId());
        question.setQuestionText(respondentData.getQuestionId());
        return question;
    }
}

